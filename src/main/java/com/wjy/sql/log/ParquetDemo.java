package com.wjy.sql.log;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.Tuple2;

public class ParquetDemo {
    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder().appName("ParquetDemo");
        builder.master("local[2]");
        builder.config("spark.driver.host","localhost");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkContext);
//        javaSparkContext.setLogLevel("ERROR");

        String path="hdfs://wang-109/tmp/result2";
//        path="D:\\devtools2\\nginx\\nginx-1.16.0\\logs\\result2";
        Dataset<Row> dataset = sparkSession.read().format("parquet").load(path);
        dataset.printSchema();
        Dataset<Row> agg = dataset.select("city", "cmsId").groupBy("city", "cmsId")
                .agg(functions.count("cmsId").as("times"))
                .orderBy(functions.col("times").desc());

        agg.show(80);

        JavaRDD<Row> rowJavaRDD = agg.javaRDD();
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = rowJavaRDD.mapToPair(row -> {
            String s = row.getAs("city") + "@@"  + row.getAs("times");
            return new Tuple2<>(s, 1);
        });
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey((v1, v2) -> v1 + v2);
//        stringIntegerJavaPairRDD1.saveAsTextFile("");
        for (Tuple2<String, Integer> stringIntegerTuple2 : stringIntegerJavaPairRDD1.collect()) {
            System.out.println(stringIntegerTuple2._1+" : "+stringIntegerTuple2._2);
        }
        sparkSession.stop();
    }
}
