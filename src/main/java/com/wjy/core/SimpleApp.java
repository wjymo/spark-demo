package com.wjy.core;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SimpleApp {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("simple").getOrCreate();
        Dataset<String> stringDataset = sparkSession.read().textFile("hdfs://wang-109/user/root1");
        long numAs = stringDataset.filter((FilterFunction<String>) s -> s.contains("a")).count();
        long numBs = stringDataset.filter((FilterFunction<String>)s -> s.contains("b")).count();
        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        sparkSession.stop();
    }
}
