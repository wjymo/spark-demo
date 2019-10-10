package com.wjy.sql.log;

import com.wjy.sql.log.entity.DayVideoAccessStat;
import com.wjy.util.MySQLUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;

public class TopNStatJob {
    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder().appName("TopNStatJob");
        builder.master("local[2]");
        builder.config("spark.driver.host","localhost");
        builder.config("spark.sql.sources.partitionColumnTypeInference.enabled","false");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

        Dataset<Row> dataset = sparkSession.read().format("parquet").load(
                "D:\\devtools2\\nginx\\nginx-1.16.0\\logs\\result2");

        String day="20190802";

//        StatDAO.deleteData(day);

        //最受欢迎的TopN课程
//        videoAccessTopNStat(sparkSession, dataset, day);

        cityAccessTopNStat(sparkSession, dataset, day);

//        dataset.show();
//        dataset
//                .select(col("day"),col("cmsId"),col("traffic"))
//                .groupBy("day")
//                .agg(sum("cmsId").as("times"))
//                .show();

//        List<String> studentInfoJSONs = new ArrayList<>();
//        studentInfoJSONs.add("{\"name\":\"Leo\", \"age\":18, \"cmsId\":18, \"cmsType\":\"课程\", \"day\":20190802}");
//        studentInfoJSONs.add("{\"name\":\"Marry\", \"age\":17, \"cmsId\":18, \"cmsType\":\"课程\", \"day\":20190802}");
//        studentInfoJSONs.add("{\"name\":\"Marry\", \"age\":17, \"cmsId\":18, \"cmsType\":\"课程\", \"day\":20190802}");
//        studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19, \"cmsId\":20, \"cmsType\":\"课程\", \"day\":20190802}");
//        studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19, \"cmsId\":20, \"cmsType\":\"课程\", \"day\":20190803}");
//        studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19, \"cmsId\":20, \"cmsType\":\"课程王\", \"day\":20190802}");
//        studentInfoJSONs.add("{\"name\":\"Leo\", \"age\":19, \"cmsId\":20, \"cmsType\":\"课程\", \"day\":20190802}");
//        JavaRDD<String> studentInfoJSONsRDD = javaSparkContext.parallelize(studentInfoJSONs);
//        Dataset<Row> json = sparkSession.read().json(studentInfoJSONsRDD);
//        Dataset<Row> dataset1 = json.select("name", "day", "cmsId", "cmsType")
//                .filter(json.col("day").equalTo(day).and(json.col("cmsType").equalTo("课程")))
//                .groupBy("day", "cmsId","name")
//                .agg(count("cmsId").as("times"))
//                .orderBy(col("times").desc())
//                ;
//        dataset1.show();


        sparkSession.stop();
    }

    private static void cityAccessTopNStat(SparkSession sparkSession, Dataset<Row> dataset, String day) {
        Dataset<Row> cityAccessTopNDF = dataset.filter(col("day").equalTo(day).and(col("cmsType").equalTo("课程")))
                .groupBy("day", "city", "cmsId")
                .agg(count("cmsId").as("times"));

        Dataset<Row> top3DF = cityAccessTopNDF.
                select(col("day"), col("city"), col("cmsId"), col("times")
                        , row_number().over(Window.partitionBy("city").orderBy(col("times").desc())).as("times_rank"))
//                .filter(col("times_rank").leq(3))
                .filter("times_rank<=3")
                ;
        top3DF.show();
    }

    private static void videoAccessTopNStat(SparkSession sparkSession, Dataset<Row> dataset, String day) {

        dataset.show(false);
        Dataset<Row> videoAccessTopNDF = dataset.filter(dataset.col("day").equalTo(day)
                .and(dataset.col("cmsType").equalTo("课程")))
                .groupBy("day", "cmsId")
                .agg(count("cmsId").as("times"))
                .orderBy(col("times").desc())
                ;
        videoAccessTopNDF.show(false);

        videoAccessTopNDF.foreachPartition(partitionOfRecords->{
            List<DayVideoAccessStat> list=new ArrayList<>();
            partitionOfRecords.forEachRemaining(row->{
                String dayItem = row.getString(0);
                Long cmsId = row.getLong(1);
                Long times = row.getLong(2);
                DayVideoAccessStat dayVideoAccessStat = new DayVideoAccessStat(dayItem, cmsId, times);
                list.add(dayVideoAccessStat);
            });
            MySQLUtils.insertDayVideoAccessTopN(list);
        });
    }
}
