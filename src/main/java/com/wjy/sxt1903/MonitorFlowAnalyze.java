package com.wjy.sxt1903;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wjy.conf.ConfigurationManager;
import com.wjy.core.WordCount;
import com.wjy.sxt1903.dao.SXTTaskDAO;
import com.wjy.sxt1903.entity.MonitorState;
import com.wjy.sxt1903.entity.SXTTask;
import com.wjy.zh13.project.constant.Constants;
import com.wjy.zh13.project.utils.DAOTools;
import com.wjy.zh13.project.utils.MyStringUtils;
import com.wjy.zh13.project.utils.ParamUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

public class MonitorFlowAnalyze {
    private static Logger logger= LoggerFactory.getLogger(MonitorFlowAnalyze.class);
    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder().appName("ParquetDemo");
        SparkContext sparkContext=null;
        SparkSession sparkSession=null;

        Boolean onLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(onLocal){
            builder.master("local[2]");
            builder.config("spark.driver.host","localhost");
             sparkSession= builder.getOrCreate();
            sparkContext= sparkSession.sparkContext();
            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

            MockData.mock(javaSparkContext, sparkSession);
        }else {

        }
        SqlSession sqlSession = DAOTools.getSession();
        SXTTaskDAO sxtTaskDAO = sqlSession.getMapper(SXTTaskDAO.class);
        Long taskId = 1l;
        SXTTask sxtTask = sxtTaskDAO.findById(taskId);
        sqlSession.close();
        if (sxtTask == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskId + "].");
            return;
        }
        String taskParams = sxtTask.getTaskParam();
        JSONObject taskParamsJsonObject = JSON.parseObject(taskParams);
        JavaRDD<Row> cameraRDDByDateRange = getCameraRDDByDateRange(sparkSession, taskParamsJsonObject);
        JavaPairRDD<String, Row> monitorId2RowRDD = cameraRDDByDateRange.mapToPair(row -> {
            String monitor_id = (String) row.getAs("monitor_id");
            return new Tuple2<>(monitor_id, row);
        });
        JavaPairRDD<String, Iterable<Row>> monitorId2RowListRDD = monitorId2RowRDD.groupByKey();

        JavaPairRDD<String, String> monitorId2CameraCountRDD = aggreagteByMonitor(monitorId2RowListRDD);
        SelfDefineAccumulator selfDefineAccumulator=new SelfDefineAccumulator();
        sparkContext.register(selfDefineAccumulator,"selfDefineAccumulator");

        JavaPairRDD<Integer, String> carCount2monitorIdRDD = checkMonitorState(sparkSession, monitorId2CameraCountRDD, selfDefineAccumulator);
        //触发job，更新累加器
        long count = carCount2monitorIdRDD.count();
        saveAccumulator(taskId,selfDefineAccumulator);

        sparkSession.stop();
    }



    private static JavaRDD<Row> getCameraRDDByDateRange(SparkSession sparkSession, JSONObject taskParamsJsonObject){
        String startDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE);

        String sql="select * from monitor_flow_action where date between ('"+startDate+"','"+endDate+"')";
        Dataset<Row> dataset = sparkSession.sql(sql);
        return dataset.javaRDD();
    }

    private static JavaPairRDD<String, String> aggreagteByMonitor(JavaPairRDD<String, Iterable<Row>>  monitorId2RowListRDD){
        return monitorId2RowListRDD.mapToPair(tuple2->{
            String monitorId = tuple2._1;
            Iterator<Row> iterator = tuple2._2.iterator();

            Set<String> set=new HashSet<>();
            Integer count=0;
            while (iterator.hasNext()){
                Row row = iterator.next();
                String camera_id = row.getAs("camera_id");
                set.add(camera_id);

                count++;
            }
            
            String infos =  Constants.FIELD_MONITOR_ID+"="+monitorId+"|"
                    +Constants.FIELD_CAMERA_IDS+"="+StringUtils.join(set.toArray(),",")+"|"
                    +Constants.FIELD_CAMERA_COUNT+"="+set.size()+"|"
                    +Constants.FIELD_CAR_COUNT+"="+count;
//            MonitorDetail.builder().monitorId(monitorId).cameraCount(set.size()).carCount(count)
//                    .cameraIds(StringUtils.join(set.toArray(),",")).build();

            return new Tuple2<>(monitorId,infos);
        });

    }

    private static JavaPairRDD<Integer, String> checkMonitorState(SparkSession sparkSession,JavaPairRDD<String, String> monitorId2CameraCountRDD
    ,SelfDefineAccumulator selfDefineAccumulator){
        String sql="SELECT * FROM monitor_camera_info";
        Dataset<Row> dataset = sparkSession.sql(sql);
        JavaRDD<Row> rowJavaRDD = dataset.javaRDD();
        JavaPairRDD<String, String> standardMonitor2CameraInfos =
                rowJavaRDD.mapToPair(row -> new Tuple2<>(row.getString(0), row.getString(1)))
                .groupByKey().mapToPair(tuple2 -> {
                    String monitorId = tuple2._1;
                    Iterable<String> cameraIds = tuple2._2;
                    Iterator<String> iterator = cameraIds.iterator();
                    StringBuilder stringBuilder = new StringBuilder();
                    Integer count=0;
                    while (iterator.hasNext()) {
                        count++;
                        String next = iterator.next();
                        if (!iterator.hasNext()) {
                            stringBuilder.append(next);
                        } else {
                            stringBuilder.append(next + ",");
                        }
                    }
                    String cameraInfos = Constants.FIELD_CAMERA_IDS+"="+stringBuilder.toString()+"|"
                            +Constants.FIELD_CAMERA_COUNT+"="+count;
                    return new Tuple2<>(monitorId,cameraInfos );
                });
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joinResultRDD =
                standardMonitor2CameraInfos.leftOuterJoin(monitorId2CameraCountRDD);
        JavaPairRDD<Integer, String> carCount2monitorIdRDD = joinResultRDD.mapPartitionsToPair(iterator -> {
            List<Tuple2<Integer, String>> list = new ArrayList<>();
            while (iterator.hasNext()) {
                Tuple2<String, Tuple2<String, Optional<String>>> next = iterator.next();
                String monitorId = next._1;
                Tuple2<String, Optional<String>> stringOptionalTuple2 = next._2;
                String standardCameraInfos = stringOptionalTuple2._1;
                Optional<String> factCameraInfosOptional = stringOptionalTuple2._2;
                String factCameraInfos = null;
                if (factCameraInfosOptional.isPresent()) {
                    factCameraInfos = factCameraInfosOptional.get();
                    int factCameraCount = Integer.parseInt(MyStringUtils.getFieldFromConcatString(factCameraInfos,
                            "\\|", Constants.FIELD_CAMERA_COUNT));
                    int standardCameraCount = Integer.parseInt(MyStringUtils.getFieldFromConcatString(standardCameraInfos,
                            "\\|", Constants.FIELD_CAMERA_COUNT));
                    if (factCameraCount == standardCameraCount) {
                        //都是正常的
                        selfDefineAccumulator.add(Constants.FIELD_NORMAL_MONITOR_COUNT + "=1|"
                                + Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + standardCameraCount + "|");
                    } else {
                        String factCameraIds = MyStringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);
                        String standardCameraIds = MyStringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);
                        List<String> factCameraIdList = Arrays.asList(factCameraIds.split(","));
                        String[] standardCameraIdsSplit = standardCameraIds.split(",");
                        Integer abnormalCameraCount = 0;
                        StringBuilder abNormalCameraStringBuilder = new StringBuilder();
                        for (String cameraId : standardCameraIdsSplit) {
                            if (!factCameraIdList.contains(cameraId)) {
                                abnormalCameraCount++;
                                abNormalCameraStringBuilder.append("," + cameraId);
                            }
                        }
                        int normalCameraCount = standardCameraIdsSplit.length - abnormalCameraCount;
                        selfDefineAccumulator.add(
                                Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|" +
                                        Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + normalCameraCount + "|" +
                                        Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + abnormalCameraCount + "|" +
                                        Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "="
                                        + "=" + monitorId + ":" + abNormalCameraStringBuilder.toString().substring(1)
                        );
                    }
                } else {
                    //optional不存在，说明此卡口下的所有摄像头都坏了
                    String standardCameraIds = MyStringUtils
                            .getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);
                    String[] split = standardCameraIds.split(",");
                    int length = split.length;
                    //abnormalMonitorCount=1|abnormalCameraCount=3|abnormalMonitorCameraInfos="0002":07553,07554,07556
                    selfDefineAccumulator.add(Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|"
                            + Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + length + "|"
                            + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + monitorId + ":" + standardCameraIds);
                }
                if (factCameraInfos != null) {
                    //从实际数据拼接到字符串中获取车流量
                    int carCount = Integer.parseInt(MyStringUtils.getFieldFromConcatString(factCameraInfos,
                            "\\|", Constants.FIELD_CAR_COUNT));
                    list.add(new Tuple2<>(carCount, monitorId));
                }
            }
            return list.iterator();
        });
        return carCount2monitorIdRDD;
    }


    private static void saveAccumulator(Long taskId, SelfDefineAccumulator selfDefineAccumulator) {
        String value = selfDefineAccumulator.value();
        String normalMonitorCount = MyStringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_NORMAL_MONITOR_COUNT);
        String normalCameraCount = MyStringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_NORMAL_CAMERA_COUNT);
        String abnormalMonitorCount = MyStringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_ABNORMAL_MONITOR_COUNT);
        String abnormalCameraCount = MyStringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_ABNORMAL_CAMERA_COUNT);
        String abnormalMonitorCameraInfos = MyStringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS);

        MonitorState monitorState = MonitorState.builder().abnormalCameraCount(abnormalCameraCount).abnormalMonitorCount(abnormalMonitorCount)
                .normalCameraCount(normalCameraCount).normalMonitorCount(normalMonitorCount)
                .abnormalMonitorCameraInfos(abnormalMonitorCameraInfos).taskId(taskId).build();

    }



}
