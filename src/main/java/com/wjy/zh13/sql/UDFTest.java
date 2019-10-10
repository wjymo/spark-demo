package com.wjy.zh13.sql;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

public class UDFTest {
    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder().appName("UDFTest");
        builder.master("local[2]");
        builder.config("spark.driver.host","localhost");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

        List<String> names= Arrays.asList("Leo", "Marry", "Jack", "Tom");
        JavaRDD<String> namesRDD = javaSparkContext.parallelize(names, 5);
        JavaRDD<String> stringJavaRDD = namesRDD.mapPartitionsWithIndex((index, iterator) -> {
            List<String> list = new ArrayList<>();
            while (iterator.hasNext()) {
                String next = iterator.next();
                System.out.println(index + " : " + next);
                list.add(next);
            }
            return list.iterator();
        }, false);
//        JavaRDD<String> stringJavaRDD = namesRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
//                LinkedList<String> linkedList = new LinkedList<String>();
//                int i = 0;
//                while (v2.hasNext())
//                    linkedList.add(Integer.toString(v1) + "|" + v2.next() + Integer.toString(i++));
//                return linkedList.iterator();
//            }
//        }, false);
//        namesRDD.mapPartitionsWithIndex((v1,v2)->{
//            LinkedList<String> linkedList = new LinkedList<String>();
//            int i = 0;
//            while (v2.hasNext())
//                linkedList.add(Integer.toString(v1) + "|" + v2.next() + Integer.toString(i++));
//            return linkedList.iterator();
//        },false);
        stringJavaRDD.collect().forEach(System.out::println);


        JavaRDD<Row> namesRowRDD = namesRDD.map(name -> RowFactory.create(name));
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = sparkSession.createDataFrame(namesRowRDD, structType);

        dataFrame.registerTempTable("names");
//        sparkSession.udf().register("strLen",new UDF1<String, Integer>() {
//                    @Override
//                    public Integer call(String in) throws Exception {
//                        return in.length();
//                    }
//                }, DataTypes.IntegerType);
        sparkSession.udf().register("strLen", (UDF1<String, Integer>) in -> in.length(), DataTypes.IntegerType);
        JavaRDD<Row> rowJavaRDD = sparkSession.sql("select name,strLen(name) from names").javaRDD();
        rowJavaRDD.collect().forEach(System.out::println);
        sparkSession.stop();
    }
}
