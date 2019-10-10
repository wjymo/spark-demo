package com.wjy.sql.log;

import com.wjy.util.AccessConvertUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkStatCleanJob {

    public static void main(String[] args) {
        if(args.length !=1) {
            System.out.println("Usage: SparkStatCleanJobYARN <inputPath> ");
            System.exit(1);
        }
        String inputPath = args[0];

        SparkSession.Builder builder = SparkSession.builder().appName("SparkStatCleanJob");
//        builder.master("local[2]");
//        builder.config("spark.driver.host","localhost");
        SparkSession sparkSession = builder.getOrCreate();

        SparkContext sparkContext =sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
//        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("D:\\devtools2\\nginx\\nginx-1.16.0\\logs\\result\\part-00000");
//        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("hdfs://wang-109:8020/user/part-00000");
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile(inputPath);

//        stringJavaRDD.take(10).forEach(System.out::println);

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField( "url", DataTypes.StringType, true ));
        structFields.add(DataTypes.createStructField( "cmsType", DataTypes.StringType, true ));
        structFields.add(DataTypes.createStructField( "cmsId", DataTypes.LongType, true ));
        structFields.add(DataTypes.createStructField( "traffic", DataTypes.LongType, true ));
        structFields.add(DataTypes.createStructField( "ip", DataTypes.StringType, true ));
        structFields.add(DataTypes.createStructField( "city", DataTypes.StringType, true ));
        structFields.add(DataTypes.createStructField( "time", DataTypes.StringType, true ));
        structFields.add(DataTypes.createStructField( "day", DataTypes.StringType, true ));
        /**
         * 2、构建StructType用于DataFrame 元数据的描述
         *
         */
        StructType structType = DataTypes.createStructType( structFields );
        Dataset<Row> dataFrame = sparkSession.createDataFrame(stringJavaRDD.map(x -> AccessConvertUtil.parseLog(x)), structType);
//
//        dataFrame.coalesce(1).write().format("parquet").mode(SaveMode.Overwrite)
//                .partitionBy("day").save("D:\\devtools2\\nginx\\nginx-1.16.0\\logs\\result2");
        dataFrame.show(100);

        sparkSession.stop();
    }
}
