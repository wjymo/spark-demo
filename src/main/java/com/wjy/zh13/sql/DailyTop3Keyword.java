package com.wjy.zh13.sql;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

public class DailyTop3Keyword {

    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder().appName("DailyUV");
        builder.master("local[2]");
        builder.config("spark.driver.host","localhost");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

        // 伪造出一份数据，查询条件
        // 备注：实际上，在实际的企业项目开发中，很可能，这个查询条件，是通过J2EE平台插入到某个MySQL表中的
        // 然后，这里呢，实际上，通常是会用Spring框架和ORM框架（MyBatis）的，去提取MySQL表中的查询条件
        Map<String, List<String>> queryParamMap = new HashMap<String, List<String>>();
        queryParamMap.put("city", Arrays.asList("beijing"));
        queryParamMap.put("platform", Arrays.asList("android"));
        queryParamMap.put("version", Arrays.asList("1.0", "1.2", "1.5", "2.0"));

        // 根据我们实现思路中的分析，这里最合适的方式，是将该查询参数Map封装为一个Broadcast广播变量
        // 这样可以进行优化，每个Worker节点，就拷贝一份数据即可
        final Broadcast<Map<String, List<String>>> queryParamMapBroadcast =javaSparkContext.broadcast(queryParamMap);
        JavaRDD<String> rawRDD = javaSparkContext.textFile(
                "G:\\BaiduNetdiskDownload\\Spark从入门到精通（Scala编程、案例实战、高级特性、Spark内核源码剖析、Hadoop高端）" +
                        "\\Spark从入门到精通（Scala编程、案例实战、高级特性、Spark内核源码剖析、Hadoop高端）\\1.sql&stream" +
                        "\\第87讲-Spark SQL：与Spark Core整合之每日top3热点搜索词统计案例实战\\文档\\keyword.txt");

        JavaRDD<String> filterRDD = rawRDD.filter(log -> {
            String[] logSplited = log.split("\t");
            String city = logSplited[3];
            String platform = logSplited[4];
            String version = logSplited[5];
            Map<String, List<String>> queryParamMapValue = queryParamMapBroadcast.getValue();

            List<String> cityList = queryParamMapValue.get("city");
            List<String> platformList = queryParamMapValue.get("platform");
            List<String> versionList = queryParamMapValue.get("version");
            if (cityList != null && cityList.size() > 0 && !cityList.contains(city)) {
                return false;
            }
            if (platformList != null && platformList.size() > 0 && !platformList.contains(platform)) {
                return false;
            }

            if (versionList != null && versionList.size() > 0 && !versionList.contains(version)) {
                return false;
            }
            return true;
        });
        JavaPairRDD<String, String> dateKeywordUserRDD = filterRDD.mapToPair(log -> {
            String[] logSplited = log.split("\t");
            String date = logSplited[0];
            String user = logSplited[1];
            String keyword = logSplited[2];
            return new Tuple2<>(date + "_" + keyword, user);
        });
        JavaPairRDD<String, Iterable<String>> dateKeywordUsersRDD = dateKeywordUserRDD.groupByKey();
        JavaPairRDD<String, Long> dateKeywordUvRDD = dateKeywordUsersRDD.mapToPair(tuple2 -> {
            String dateKeyword = tuple2._1;
            Iterator<String> iterator = tuple2._2.iterator();
            // 对用户进行去重，并统计去重后的数量
            List<String> distinctUsers = new ArrayList<String>();

            while (iterator.hasNext()) {
                String next = iterator.next();
                if (!distinctUsers.contains(next)) {
                    distinctUsers.add(next);
                }
            }
            // 获取uv
            long uv = distinctUsers.size();
            return new Tuple2<String, Long>(dateKeyword, uv);
        });
        JavaRDD<Row> dateKeywordUvRowRDD = dateKeywordUvRDD.map(dateKeywordUv -> {
            String date = dateKeywordUv._1.split("_")[0];
            String keyword = dateKeywordUv._1.split("_")[1];
            return RowFactory.create(date, keyword, dateKeywordUv._2);
        });


        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyword", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> dataset = sparkSession.createDataFrame(dateKeywordUvRowRDD, structType);
        dataset.registerTempTable("daily_keyword_uv");
        Dataset<Row> dailyTop3KeywordDF = sparkSession.sql("SELECT date,keyword,uv from " +
                " ( select date,keyword,uv," +
                "row_number() over (partition by date order by uv desc)rank  FROM daily_keyword_uv ) tmp WHERE rank<=3");

//        dailyTop3KeywordDF.printSchema();
//        dailyTop3KeywordDF.show();
        JavaRDD<Row> dailyTop3KeywordRDD = dailyTop3KeywordDF.javaRDD();
        JavaPairRDD<String, String> top3DateKeywordUvRDD = dailyTop3KeywordRDD.mapToPair(row -> {
            String date = String.valueOf(row.get(0));
            String keyword = String.valueOf(row.get(1));
            Long uv = Long.valueOf(String.valueOf(row.get(2)));
            return new Tuple2<String, String>(date, keyword + "_" + uv);
        });
        JavaPairRDD<String, Iterable<String>> top3DateKeywordsRDD = top3DateKeywordUvRDD.groupByKey();
        JavaPairRDD<Long, String> uvDateKeywordsRDD = top3DateKeywordsRDD.mapToPair(tuple -> {
            String date = tuple._1;
            Long totalUv = 0L;
            String dateKeywords = date;

            Iterator<String> keywordUvIterator = tuple._2.iterator();
            while (keywordUvIterator.hasNext()) {
                String keywordUv = keywordUvIterator.next();

                Long uv = Long.valueOf(keywordUv.split("_")[1]);
                totalUv += uv;

                dateKeywords += "," + keywordUv;
            }

            return new Tuple2<Long, String>(totalUv, dateKeywords);
        });

//        uvDateKeywordsRDD.foreach(tuple->{
//            System.out.println(tuple._1+" : "+tuple._2);
//        });
        JavaPairRDD<Long, String> sortedUvDateKeywordsRDD = uvDateKeywordsRDD.sortByKey(false);
        JavaRDD<Row> sortedRowRDD = sortedUvDateKeywordsRDD.flatMap(tuple -> {
            String dateKeywords = tuple._2;
            String[] dateKeywordsSplited = dateKeywords.split(",");
            String date = dateKeywordsSplited[0];

            List<Row> rows = new ArrayList<>();
            rows.add(RowFactory.create(date,
                    dateKeywordsSplited[1].split("_")[0],
                    Long.valueOf(dateKeywordsSplited[1].split("_")[1])));
            rows.add(RowFactory.create(date,
                    dateKeywordsSplited[2].split("_")[0],
                    Long.valueOf(dateKeywordsSplited[2].split("_")[1])));
            rows.add(RowFactory.create(date,
                    dateKeywordsSplited[3].split("_")[0],
                    Long.valueOf(dateKeywordsSplited[3].split("_")[1])));

            return rows.iterator();
        });


//        JavaRDD<Row> sortedRowRDD = sortedUvDateKeywordsRDD.flatMap(
//
//                new FlatMapFunction<Tuple2<Long,String>, Row>() {
//
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public Iterator<Row> call(Tuple2<Long, String> tuple)
//                            throws Exception {
//                        String dateKeywords = tuple._2;
//                        String[] dateKeywordsSplited = dateKeywords.split(",");
//
//                        String date = dateKeywordsSplited[0];
//
//                        List<Row> rows = new ArrayList<Row>();
//                        rows.add(RowFactory.create(date,
//                                dateKeywordsSplited[1].split("_")[0],
//                                Long.valueOf(dateKeywordsSplited[1].split("_")[1])));
//                        rows.add(RowFactory.create(date,
//                                dateKeywordsSplited[2].split("_")[0],
//                                Long.valueOf(dateKeywordsSplited[2].split("_")[1])));
//                        rows.add(RowFactory.create(date,
//                                dateKeywordsSplited[3].split("_")[0],
//                                Long.valueOf(dateKeywordsSplited[3].split("_")[1])));
//                        return rows.iterator();
//                    }
//                });

        sparkSession.stop();
    }
}
