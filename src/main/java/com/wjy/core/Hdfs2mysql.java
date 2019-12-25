package com.wjy.core;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.INT;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Hdfs2mysql {
    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder();
        builder.master("local[2]");
        builder.appName("Hdfs2mysql");
        builder.config("spark.driver.host","localhost");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

        JavaRDD<String> logRDD = javaSparkContext.textFile("hdfs://wang-108:8020/tmp/email.txt");
        JavaRDD<String> newRDD = logRDD.map(line -> {
            String[] strings = line.split("\t");
            System.out.println(strings.length);
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < strings.length; i++) {
                String string = strings[i];
                if (StringUtils.isNotBlank(string) && !StringUtils.equals(string, "-") && !StringUtils.equals(string, "\"-\"")) {
                    stringBuilder.append(string);
                    if (i != strings.length - 1) {
                        stringBuilder.append("\t");
                    }
                }
            }
            return stringBuilder.toString();
        });
        newRDD.foreachPartition(part->{
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:mysql://172.168.1.108:33068/spark-demo", "root", "123456");
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            String sql="insert into gooal (id,`name`,email) values (?,?,?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            part.forEachRemaining(line->{
                String[] split = line.split("\t");
                try {
                    preparedStatement.setInt(1,Integer.parseInt(split[0]));
                    preparedStatement.setString(2,split[1]);
                    preparedStatement.setString(3,split[2]);
                    preparedStatement.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            preparedStatement.executeBatch();
            connection.commit();
        });

//        JavaRDD<String> flatMap = logRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//
//
//        flatMap.foreach(word->log.info(word));


        sparkSession.stop();
    }
}
