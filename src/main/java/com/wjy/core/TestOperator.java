package com.wjy.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestOperator {
    public static void main(String[] args) {
//        mapPartitions();
//        cogroup();
        leftOuterJoin();

    }

    private static void leftOuterJoin() {
        SparkSession sparkSession = SparkSession.builder().appName("leftOuterJoin")
                .master("local[2]")
                .config("spark.driver.host","localhost")
                .getOrCreate();
        SparkContext sparkContext =sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

        List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
        blacklist.add(new Tuple2<String, Boolean>("tom", true));
        JavaPairRDD<String, Boolean> blacklistRDD = javaSparkContext.parallelizePairs(blacklist);

        List<Tuple2<String, Integer>> blacklist2 = new ArrayList<Tuple2<String, Integer>>();
        blacklist2.add(new Tuple2<String, Integer>("tom", 111));
        blacklist2.add(new Tuple2<String, Integer>("huyao", 222));
        JavaPairRDD<String, Integer> blacklistRDD2 = javaSparkContext.parallelizePairs(blacklist2);

//        JavaPairRDD<String, Tuple2<Boolean, Optional<Integer>>> stringTuple2JavaPairRDD = blacklistRDD.leftOuterJoin(blacklistRDD2);
        JavaPairRDD<String, Tuple2<Integer, Optional<Boolean>>> stringTuple2JavaPairRDD = blacklistRDD2.leftOuterJoin(blacklistRDD);
        stringTuple2JavaPairRDD.collect().forEach(System.out::println);


        sparkSession.stop();
    }

    private static void cogroup() {
        SparkSession sparkSession = SparkSession.builder().appName("cogroup")
                .master("local[2]")
                .config("spark.driver.host","localhost")
                .getOrCreate();
        SparkContext sparkContext =sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(1, 70),
                new Tuple2<Integer, Integer>(2, 80),
                new Tuple2<Integer, Integer>(3, 50));

        // 并行化两个RDD
        JavaPairRDD<Integer, String> students = javaSparkContext.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = javaSparkContext.parallelizePairs(scoreList);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores =
                students.cogroup(scores);
        studentScores.collect().forEach(System.out::println);

        sparkSession.stop();
    }

    private static void mapPartitions(){
        SparkSession sparkSession = SparkSession.builder().appName("TestOperator")
                .master("local[2]")
                .config("spark.driver.host","localhost")
                .getOrCreate();
        SparkContext sparkContext =sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("D:\\devtools2\\nginx\\nginx-1.16.0\\logs\\result\\xx.txt");
        JavaRDD<String> stringJavaRDDIterator = stringJavaRDD.flatMap(line -> {
            String[] split = line.split("\t");
            return Arrays.asList(split).iterator();
        });
        JavaRDD<String> stringJavaRDDMapPartitions = stringJavaRDDIterator.mapPartitions(iterator -> {
            List<String> list=new ArrayList<>();
            while (iterator.hasNext()) {
                String next = iterator.next();
                if (!StringUtils.contains(next, "Mozilla")) {
                    list.add(next);
                }
            }
            return list.iterator();
        });

        stringJavaRDDMapPartitions.collect().forEach(System.out::println);
        sparkSession.stop();
    }
}
