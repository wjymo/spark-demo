package com.wjy.pk.batch;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ImoocLogApp {
    public static void main(String[] args) {
        SparkSession.Builder builder=SparkSession.builder().appName("JDBCDataSource");
        builder.master("local[2]");
        builder.config("spark.driver.host","localhost");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("D:\\workspace\\coding357\\spark进阶安装包\\access.test.log");
        JavaRDD<String> map = stringJavaRDD.map(line -> {
            StringBuilder stringBuilder = new StringBuilder();
            String[] split = line.split(" ");
            String ip = split[0];
//            String time = split[3] + split[4];
//            String method = split[5];
//            String url = split[6];
//            String protocal = split[7];
//            String code = split[8];
//            String referer = split[11];
//            String ua = split[13]+split[14]+split[15]+split[16]+split[17]+split[18]+split[19]+split[20]+split[21]+split[22]+
//                    split[23]+split[24]+split[25]+split[26]+split[27];
//            stringBuilder.append(ip).append("\t").append(time).append("\t").append(method).append("\t").append(url).append("\t")
//                    .append(protocal).append("\t").append(code).append("\t").append(referer).append("\t").append(ua);
            return stringBuilder.append(split.length).toString();
        });
        map.collect().forEach(System.out::println);


        sparkSession.stop();
    }
}
