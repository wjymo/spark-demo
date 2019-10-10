package com.wjy.zh13.sql;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JSONDataSource {
    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder().appName("JSONDataSource");
        builder.master("local[2]");
        builder.config("spark.driver.host","localhost");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

        Dataset<Row> dataset = sparkSession.read().json(
                "G:\\BaiduNetdiskDownload\\Spark从入门到精通（Scala编程、案例实战、高级特性、Spark内核源码剖析、Hadoop高端）" +
                        "\\Spark从入门到精通（Scala编程、案例实战、高级特性、Spark内核源码剖析、Hadoop高端）\\1.sql&stream" +
                        "\\第80讲-Spark SQL：JSON数据源复杂综合案例实战\\文档\\students.json");

        dataset.registerTempTable("student_scores");
        Dataset<Row> gooStudentDataset = sparkSession.sql("select name,score from student_scores where score>=80");
        List<String> names= gooStudentDataset.javaRDD().map(row -> row.getString(0)).collect();
        List<String> studentInfoJSONs = new ArrayList<>();
        studentInfoJSONs.add("{\"name\":\"Leo\", \"age\":18}");
        studentInfoJSONs.add("{\"name\":\"Marry\", \"age\":17}");
        studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19}");


        JavaRDD<String> studentInfoJSONsRDD = javaSparkContext.parallelize(studentInfoJSONs);
        Dataset<Row> studentInfosDS = sparkSession.read().json(studentInfoJSONsRDD);
        studentInfosDS.registerTempTable("student_infos");
        StringBuilder sqlStringBuilder=new StringBuilder();
        sqlStringBuilder.append("select name,age from student_infos where name in (");
        for (int i=0;i<names.size();i++){
            sqlStringBuilder.append("'" + names.get(i) + "'");
            if(i<names.size()-1){
                sqlStringBuilder.append(",");
            }
        }
        sqlStringBuilder.append(")");
        Dataset<Row> goodStudentInfosDS = sparkSession.sql(sqlStringBuilder.toString());


        JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsRDD =
                gooStudentDataset.javaRDD().mapToPair(row ->
                        new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1)))))
                .join(goodStudentInfosDS.javaRDD().mapToPair(row ->
                        new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))))));
        JavaRDD<Row> goodStudentRowsRDD = goodStudentsRDD.map(tuple -> RowFactory.create(tuple._1, tuple._2._1, tuple._2._2));

        // 创建一份元数据，将JavaRDD<Row>转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> goodStudentsDS = sparkSession.createDataFrame(goodStudentRowsRDD, structType);
        goodStudentsDS.coalesce(1).write().format("json").mode(SaveMode.Overwrite).save("D:\\devtools2\\nginx\\nginx-1.16.0\\logs\\test_json");


        sparkSession.stop();

    }
}
