package com.wjy.zh13.sql;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
//import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DailyUV {
    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder().appName("DailyUV");
        builder.master("local[2]");
        builder.config("spark.driver.host","localhost");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

        List<String> userAccessLog = Arrays.asList(
                "2015-10-01,1122",
                "2015-10-01,1122",
                "2015-10-01,1123",
                "2015-10-01,1124",
                "2015-10-01,1124",
                "2015-10-02,1122",
                "2015-10-02,1121",
                "2015-10-02,1123",
                "2015-10-02,1123");
        JavaRDD<String> userAccessLogRDD = javaSparkContext.parallelize(userAccessLog);

        JavaRDD<Row> userAccessLogRowRDD = userAccessLogRDD.map(log -> {
            String[] split = log.split(",");
            return RowFactory.create(split[0], Integer.valueOf(split[1].toString()));
        });
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("userId", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> dataset = sparkSession.createDataFrame(userAccessLogRowRDD, structType);
        Dataset<Row> agg = dataset.groupBy("date").agg(dataset.col("date"), functions.countDistinct("userId"));
        agg.show();

        sparkSession.stop();
    }
}
