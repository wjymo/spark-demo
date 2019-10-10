package com.wjy.zh13.project.session;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.wjy.zh13.project.constant.*;
import com.wjy.zh13.project.dao.SessionAggrStatDAO;
import com.wjy.zh13.project.dao.TaskDAO;
import com.wjy.zh13.project.entity.SessionAggrStat;
import com.wjy.zh13.project.entity.Task;
import com.wjy.zh13.project.test.MockData;
import com.wjy.zh13.project.utils.*;
import org.apache.ibatis.session.SqlSession;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.*;

public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder();
        builder.appName("UserVisitSessionAnalyzeSpark");
        builder.master("local[1]");
        builder.config("spark.driver.host", "localhost");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

        MockData.mock(sparkSession);

        SqlSession sqlSession = DAOTools.getSession();
        TaskDAO taskDAO = sqlSession.getMapper(TaskDAO.class);
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
        Task task = taskDAO.findById(taskId);
        sqlSession.close();
        if (task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskId + "].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sparkSession, taskParam);
        JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);

        JavaPairRDD<String, String> sessionid2AggrInfoRDD =
                aggregateBySession(sparkSession, sessionid2actionRDD);
//        sessionid2actionRDD.collect().forEach(System.out::println);
//        sessionid2actionRDD.foreach(tuple->{
//            Row row = tuple._2;
//            String string = row.getString(4);
//            Date date = DateUtils.parseTime(string);
//            System.out.println("date: "+date);
//        });

//        Accumulator<String> sessionAggrStatAccumulator = javaSparkContext.accumulator(
//                "", new SessionAggrStatAccumulator());
        SessionAggrStatAccumulator2 sessionAggrStatAccumulator = new SessionAggrStatAccumulator2();
        sparkContext.register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator");

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
        filteredSessionid2AggrInfoRDD.collect().forEach(System.out::println);

        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
                filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
//        sessionid2detailRDD.collect().forEach(System.out::println);


//        long count = sessionid2detailRDD.count();
//        System.out.println("count: "+count);
        // 计算出各个范围的session占比，并写入MySQL
//        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
//                task.getTaskId());


        sparkSession.stop();
    }


    /**
     * 获取指定日期范围内的用户行为数据RDD
     */
    public static JavaRDD<Row> getActionRDDByDateRange(SparkSession sparkSession, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql =
                "select * "
                        + "from user_visit_action "
                        + "where date>='" + startDate + "' "
                        + "and date<='" + endDate + "'";
//				+ "and session_id not in('','','')"

        Dataset<Row> actionDF = sparkSession.sql(sql);

        /**
         * 这里就很有可能发生上面说的问题
         * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
         * 实际上，你需要1000个task去并行执行
         *
         * 所以说，在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
         */

//		return actionDF.javaRDD().repartition(1000);

        JavaRDD<Row> rowJavaRDD = actionDF.javaRDD();
//        rowJavaRDD.mapPartitionsWithIndex(new JFunction2);
        return rowJavaRDD;
    }

    /**
     * 获取sessionid2到访问行为数据的映射的RDD
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
//        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Iterator<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
//                List<Tuple2<String, Row>> list = new ArrayList<>();
//                while(iterator.hasNext()) {
//                    Row row = iterator.next();
//                    list.add(new Tuple2<>(row.getString(2), row));
//                }
//
//                return list.iterator();
//            }
//
//        });
        return actionRDD.mapToPair(row ->
                new Tuple2<>(row.getString(2), row));
    }

    private static JavaPairRDD<String, String> aggregateBySession(SparkSession sparkSession,
                                                                  JavaPairRDD<String, Row> sessinoid2actionRDD) {
        // 对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessinoid2actionRDD.groupByKey();

        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(tuple -> {
            String sessionId = tuple._1;
            Iterator<Row> iterator = tuple._2.iterator();

            StringBuffer searchKeywordsBuffer = new StringBuffer("");
            StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

            Long userId = null;

            // session的起始和结束时间
            Date startTime = null;
            Date endTime = null;
            // session的访问步长
            int stepLength = 0;

            // 遍历session所有的访问行为
            while (iterator.hasNext()) {
                // 提取每个访问行为的搜索词字段和点击品类字段
                Row row = iterator.next();
                if (userId == null) {
                    userId = row.getLong(1);
                }
                String searchKeyword = row.getString(5);
                Long clickCategoryId = row.get(6) == null ? null : row.getLong(6);

                // 实际上这里要对数据说明一下
                // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                // 其实，只有搜索行为，是有searchKeyword字段的
                // 只有点击品类的行为，是有clickCategoryId字段的
                // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                // 我们决定是否将搜索词或点击品类id拼接到字符串中去
                // 首先要满足：不能是null值
                // 其次，之前的字符串中还没有搜索词或者点击品类id

                if (MyStringUtils.isNotEmpty(searchKeyword)) {
                    if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                        searchKeywordsBuffer.append(searchKeyword + ",");
                    }
                }
                if (clickCategoryId != null) {
                    if (!clickCategoryIdsBuffer.toString().contains(
                            String.valueOf(clickCategoryId))) {
                        clickCategoryIdsBuffer.append(clickCategoryId + ",");
                    }
                }

                // 计算session开始和结束时间
                String actionTimeString = row.getString(4);
                System.out.println("actionTimeString: " + actionTimeString);
                Date actionTime = DateUtils.parseTime(actionTimeString);

                if (startTime == null) {
                    startTime = actionTime;
                }
                if (endTime == null) {
                    endTime = actionTime;
                }

                if (actionTime.before(startTime)) {
                    startTime = actionTime;
                }
                if (actionTime.after(endTime)) {
                    endTime = actionTime;
                }

                // 计算session访问步长
                stepLength++;
            }

            String searchKeywords = MyStringUtils.trimComma(searchKeywordsBuffer.toString());
            String clickCategoryIds = MyStringUtils.trimComma(clickCategoryIdsBuffer.toString());

            // 计算session访问时长（秒）
            long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

            // 大家思考一下
            // 我们返回的数据格式，即使<sessionid,partAggrInfo>
            // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
            // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
            // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
            // 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
            // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

            // 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
            // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
            // 然后再直接将返回的Tuple的key设置成sessionid
            // 最后的数据格式，还是<sessionid,fullAggrInfo>

            // 聚合数据，用什么样的格式进行拼接？
            // 我们这里统一定义，使用key=value|key=value
            String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                    + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                    + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds
//                    ;
                    + "|"
                    + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                    + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                    + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

            return new Tuple2<Long, String>(userId, partAggrInfo);
        });

        // 查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sparkSession.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(row ->
                new Tuple2<Long, Row>(row.getLong(0), row));
        // 将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);

        // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair(tuple -> {
            String partAggrInfo = tuple._2._1;
            Row userInfoRow = tuple._2._2;

            String sessionid = MyStringUtils.getFieldFromConcatString(
                    partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

            int age = userInfoRow.getInt(3);
            String professional = userInfoRow.getString(4);
            String city = userInfoRow.getString(5);
            String sex = userInfoRow.getString(6);

            String fullAggrInfo = partAggrInfo + "|"
                    + Constants.FIELD_AGE + "=" + age + "|"
                    + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                    + Constants.FIELD_CITY + "=" + city + "|"
                    + Constants.FIELD_SEX + "=" + sex;

            return new Tuple2<String, String>(sessionid, fullAggrInfo);
        });
        return sessionid2FullAggrInfoRDD;
    }


    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            AccumulatorV2 sessionAggrStatAccumulator
//            ,final Accumulator<String> sessionAggrStatAccumulator
    ) {
        // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // 此外，这里其实大家不要觉得是多此一举
        // 其实我们是给后面的性能优化埋下了一个伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

        if (_parameter.endsWith("|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

                new Function<Tuple2<String, String>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        // 首先，从tuple中，获取聚合数据
                        String aggrInfo = tuple._2;

                        // 接着，依次按照筛选条件进行过滤
                        // 按照年龄范围进行过滤（startAge、endAge）
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        // 按照职业范围进行过滤（professionals）
                        // 互联网,IT,软件
                        // 互联网
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 按照性别进行过滤
                        // 男/女
                        // 男，女
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照搜索词进行过滤
                        // 我们的session可能搜索了 火锅,蛋糕,烧烤
                        // 我们的筛选条件可能是 火锅,串串香,iphone手机
                        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                        // 任何一个搜索词相当，即通过
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                                parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        // 按照点击品类id进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                        // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
                        // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                        // 进行相应的累加计数

                        // 主要走到这一步，那么就是需要计数的session
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        // 计算出session的访问时长和访问步长的范围，并进行相应的累加
                        try {
                            long visitLength = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                                    aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                            calculateVisitLength(visitLength);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        long stepLength = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                        calculateStepLength(stepLength);

                        return true;
                    }

                    /**
                     * 计算访问时长范围
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 计算访问步长范围
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }

                });

        return filteredSessionid2AggrInfoRDD;
    }


    /**
     * 获取通过筛选条件的session的访问明细数据RDD
     *
     * @param sessionid2aggrInfoRDD
     * @param sessionid2actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2detailRDD(
            JavaPairRDD<String, String> sessionid2aggrInfoRDD, JavaPairRDD<String, Row> sessionid2actionRDD) {

        JavaPairRDD<String, Tuple2<String, Row>> join = sessionid2aggrInfoRDD
                .join(sessionid2actionRDD);
        JavaPairRDD<String, Row> sessionid2detailRDD = join
                .mapToPair(tuple -> new Tuple2(tuple._1, tuple._2._2));
        return sessionid2detailRDD;
    }


    /**
     * 随机抽取session
     *
     * @param sessionid2AggrInfoRDD
     */
    private static void randomExtractSession(
            JavaSparkContext sc,
            final long taskid,
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2actionRDD) {
        // 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(tuple -> {
            String aggrInfo = tuple._2;
            String startTime = MyStringUtils.getFieldFromConcatString(
                    aggrInfo, "\\|", Constants.FIELD_START_TIME);
            String dateHour = DateUtils.getDateHour(startTime);
            return new Tuple2<String, String>(dateHour, aggrInfo);
        });
        Map<String, Long> countMap = time2sessionidRDD.countByKey();
        // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
//        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();
        Table<String, String, Long> dateHourCountMap = HashBasedTable.create();

        for (Map.Entry<String, Long> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long count = countEntry.getValue();
            dateHourCountMap.put(date,hour,count);
        }

        // 总共要抽取100个session，先按照天数，进行平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();
        Map<String, Map<String, List<Integer>>> dateHourExtractMap =new HashMap<>();
        Random random = new Random();
        for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.rowMap().entrySet()){
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
            // 计算出这一天的session总数
            long sessionCount = hourCountMap.values().size();
//            for(long hourCount : hourCountMap.values()) {
//                sessionCount += hourCount;
//            }
            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if(hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            // 遍历每个小时
            for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
                // 就可以计算出，当前小时需要抽取的session数量
                int hourExtractNumber = (int)(((double)count / (double)sessionCount)
                        * extractNumberPerDay);
                if(hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

            }
        }
    }


    /**
     * 计算各session范围占比，并写入MySQL
     *
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(MyStringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = 0;
        try {
            visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                    (double) visit_length_1s_3s / (double) session_count, 2);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        SqlSession sqlSession = DAOTools.getSession();
        SessionAggrStatDAO sessionAggrStatDAO = sqlSession.getMapper(SessionAggrStatDAO.class);
        sessionAggrStatDAO.insert(sessionAggrStat);
        sqlSession.commit();
        sqlSession.close();
    }
}
