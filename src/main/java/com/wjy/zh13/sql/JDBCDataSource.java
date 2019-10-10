package com.wjy.zh13.sql;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCDataSource {
    public static void main(String[] args) {
        SparkSession.Builder builder=SparkSession.builder().appName("JDBCDataSource");
        builder.master("local[2]");
        builder.config("spark.driver.host","localhost");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

        Map<String, String> options = new HashMap<String, String>();
        String jdbcUrl="jdbc:mysql://wang-108:33068/testdb";
        options.put("url", jdbcUrl);
        options.put("dbtable", "student_infos");
        options.put("user", "root");
        options.put("password", "123456");
        Dataset<Row> studentInfosDF = sparkSession.read().format("jdbc")
                .options(options).load();
        options.put("dbtable", "student_scores");
        Dataset<Row> studentScoresDF = sparkSession.read().format("jdbc")
                .options(options).load();

        JavaPairRDD<String, Tuple2<Integer, Integer>> studentsRDD =
                studentInfosDF.javaRDD().mapToPair(row->
                        new Tuple2<String, Integer>(row.getString(0),Integer.valueOf(String.valueOf(row.get(1))))
        ).join(studentScoresDF.javaRDD().mapToPair(row->
                        new Tuple2<String, Integer>(String.valueOf(row.get(0)),Integer.valueOf(String.valueOf(row.get(1))))));

        JavaRDD<Row> studentRowsRDD = studentsRDD.map(tuple -> RowFactory.create(tuple._1, tuple._2._1, tuple._2._2));

        JavaRDD<Row> filteredStudentRowsRDD = studentRowsRDD.filter(row -> row.getInt(2) > 80);

//        filteredStudentRowsRDD.collect().forEach(System.out::println);
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> studentsDF = sparkSession.createDataFrame(filteredStudentRowsRDD, structType);

//        Row[] collect = studentsDF.collect();
//        for(Row row : rows) {
//            System.out.println(row);
//        }

        studentsDF.javaRDD().foreach(row->{
            String sql = "insert into good_student_infos values("
                    + "'" + String.valueOf(row.getString(0)) + "',"
                    + Integer.valueOf(String.valueOf(row.get(1))) + ","
                    + Integer.valueOf(String.valueOf(row.get(2))) + ")";
            Class.forName("com.mysql.jdbc.Driver");

            Connection conn = null;
            PreparedStatement pstmt = null;
            try {
                conn = DriverManager.getConnection(
                        jdbcUrl, "root", "123456");
                pstmt = conn.prepareStatement(sql);
                pstmt.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if(pstmt != null) {
                    pstmt.close();
                }
                if(conn != null) {
                    conn.close();
                }
            }
        });

        sparkSession.stop();
    }


}
