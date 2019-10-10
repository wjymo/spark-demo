package com.wjy.zhenaiETL;

import com.wjy.util.FTPUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//sql:
//SELECT userId ,count(userId) count,GROUP_CONCAT(id SEPARATOR ',') ids  FROM user_profile GROUP BY userId HAVING count>1 ORDER BY count desc;
@Slf4j
public class ZhenaiETLDemo {
    public static void main(String[] args) {
        SparkSession.Builder builder=SparkSession.builder().appName("ZhenaiETLDemo");
        builder.master("local[2]");
        builder.config("spark.driver.host","localhost");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

        Map<String, String> options = new HashMap<>();
        String jdbcUrl="jdbc:mysql://wang-108:33068/zhenai";
        options.put("url", jdbcUrl);
        options.put("dbtable", "user_profile");
        options.put("user", "root");
        options.put("password", "123456");
        Dataset<Row> userDF = sparkSession.read().format("jdbc")
                .options(options).load();

        userDF.show();
        JavaRDD<Row> userRDD = userDF.javaRDD();
        List<Row> take = userRDD.take(10);

        take.forEach(row->{
            String userId = row.getString(0);
            String city = row.getString(1);
            String username = row.getString(2);
            String imgUrl = row.getString(3);
            String age = row.getString(4);
            String signatureStr = row.getString(5);
            String maritalStatus = row.getString(6);
            String constellation = row.getString(7);
            String heigt = row.getString(8);
            String weight = row.getString(9);
            String workplace = row.getString(10);
            String income = row.getString(11);
            String job = row.getString(12);
            String education = row.getString(13);
            String interests = row.getString(14);
            int id = row.getInt(15);
            String baseContent = row.getString(16);
            String nation = row.getString(17);
            String birthplace = row.getString(18);
            String bodyType = row.getString(19);
            String smoke = row.getString(20);
            String drink = row.getString(21);
            String house = row.getString(22);
            String car = row.getString(23);
            String children = row.getString(24);
            String toHaveChildren = row.getString(25);
            String marriedTime = row.getString(26);
            String extraContent = row.getString(27);
            String criteriaContent = row.getString(28);


            if(StringUtils.isNotEmpty(baseContent)){
                String[] baseSplit = baseContent.split("||");
                //离异||38岁||射手座(11.22-12.21)||165cm||工作地:安庆宜秀区||月收入:3千以下||销售总监||高中及以下||
                for (String s : baseSplit) {
                    if(s.contains("岁")){
                        age=s;
                    }
                }
            }


            String imgName=username+"-"+userId+".jpg";

            URL httpurl = null;
            try {
                httpurl = new URL(imgUrl);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
            String dir="D:\\images\\";
            File file = new File(dir + imgName);
            try {
                FileUtils.copyURLToFile(httpurl, file);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Boolean upload = FTPUtils.upload(imgName, dir + imgName);
            if(upload){
                file.delete();
            }
            log.info(""+row);
        });

        sparkSession.stop();

    }
}
