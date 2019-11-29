package com.wjy.util;

//import com.mongodb.MongoClient;
//import com.mongodb.ServerAddress;
//import com.mongodb.client.*;
//import com.mongodb.client.model.UpdateOneModel;
//import com.mongodb.client.model.WriteModel;
//import org.bson.Document;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Main {
    private static String CPU="cpu";
    private static String MEMORY="memory";
    public static void main(String[] args) throws IOException {


//        MongoCredential credential = MongoCredential.createCredential("root", "admin", "gooalgene@Mongo123$".toCharArray());
//        MongoClient mongo=new MongoClient(new ServerAddress("172.168.1.29",27117), Arrays.asList(credential));
//        MongoClient mongo=new MongoClient(new ServerAddress("172.168.1.109",27017));
//        MongoIterable<String> dbsIterable = mongo.listDatabaseNames();
//        MongoCursor<String> iterator = dbsIterable.iterator();
//        while (iterator.hasNext()){
//            String next = iterator.next();
//            if(next.endsWith("db")){
//                MongoDatabase database = mongo.getDatabase(next);
//                MongoCollection<Document> collection = database.getCollection("classifys");
//                Document filter = new Document();
//                filter.append("name", "fruiting body_all");
//                FindIterable<Document> documents = collection.find(filter);
//
//                List<WriteModel<Document>> writeModelList = new ArrayList<>();
//
//                MongoCursor<Document> documentMongoCursor = documents.iterator();
//                while (documentMongoCursor.hasNext()){
//                    Document document = documentMongoCursor.next();
//                    List<Document> list = (List) document.get("children");
//                    for(Document child:list){
//                        String name = (String)child.get("name");
//                        String chinese = (String) child.get("chinese");
//
////                    Bson updateBson = Filters.eq("children.chinese", chinese);
//                        Document filter2 = new Document();
//                        filter2.append("name", "fruiting body_all");
//                        filter2.append("children.name", name);
//                        Document updateDocument = new Document();
//                        updateDocument.append("$set", new Document("children.$.chinese", name));
//
////                    collection.updateOne(filter,updateDocument);
//                        UpdateOneModel<Document> updateOneModel = new UpdateOneModel<>(filter2, updateDocument);
//                        writeModelList.add(updateOneModel);
//                    }
//                }
//                if(!writeModelList.isEmpty()){
//                    System.out.println(String.format("集合【%s】被更新，被更新子children有：%d个",next,writeModelList.size()));
//                    collection.bulkWrite(writeModelList);
//                }
//            }
//        }
//        mongo.close();

        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//        InputStream inputStream = Main.class.getClassLoader().getResourceAsStream("CloudEye_cn-north-4_2019-10-11T20-10-51Z.json");
        String path="F:\\wjy工作\\工作\\生信监控\\JSON";
        File file=new File(path);
//        List<String> jsons=new ArrayList<>();
//        List<Double> sumList=new ArrayList<>();
        List<BigDecimal> sumList=new ArrayList<>();
        Map<String,List<JSONArray>> indexMap=new HashMap<>();
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File fileItem : files) {
                File[] files1 = fileItem.listFiles();
                for (File file1 : files1) {
                    InputStream inputStream=new FileInputStream(file1);
                    BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(inputStream),4 * 1024);
                    StringBuilder stringBuilder=new StringBuilder();
                    String line =null;
                    while ((line=bufferedReader.readLine())!=null){
                        stringBuilder.append(line);
                    }
                    String s = stringBuilder.toString();
                    JSONObject jsonObject =JSON.parseObject(s);
                    JSONArray metrics = jsonObject.getJSONArray("metrics");
                    for (Object metric : metrics) {
                        JSONObject metricJson = (JSONObject) metric;
                        String metricStr = (String) metricJson.get("metric");
                        if (StringUtils.isNotEmpty(metricStr)) {
                            JSONArray data = metricJson.getJSONArray("data");
                            if(( metricStr.contains("弹性云服务器.CPU使用率"))) {
                                List<JSONArray> jsonArrays = indexMap.get(CPU);
                                if(jsonArrays==null){
                                    jsonArrays=new ArrayList<>();
                                    indexMap.put(CPU,jsonArrays);
                                }
                                jsonArrays.add(data);
                            }
                            if(metricStr.contains("内存使用率")){
                                List<JSONArray> jsonArrays = indexMap.get(MEMORY);
                                if(jsonArrays==null){
                                    jsonArrays=new ArrayList<>();
                                    indexMap.put(MEMORY,jsonArrays);
                                }
                                jsonArrays.add(data);
                            }
                        }

                    }

//                    List<JSONArray> list = metrics.stream().filter(metric -> {
//                        JSONObject metricJson = (JSONObject) metric;
//                        String metricStr = (String) metricJson.get("metric");
//                        if (StringUtils.isNotEmpty(metricStr) &&
//                                ( metricStr.contains("弹性云服务器.CPU使用率"))) {
//                            return true;
//                        }
//                        return false;
//                    }).map(metric -> {
//                        JSONObject metricJson = (JSONObject) metric;
//                        JSONArray data = metricJson.getJSONArray("data");
//                        return data;
//                    }).collect(Collectors.toList());
//                    JSONArray jsonArray = list.get(0);
//                    int size = jsonArray.size();
//                    BigDecimal bigDecimal=new BigDecimal(0);
//                    for (Object o : jsonArray) {
//                        JSONObject item = (JSONObject) o;
//                        BigDecimal value = (BigDecimal)item.get("value");
//                        bigDecimal=bigDecimal.add(value);
//                    }
//                    BigDecimal divide = bigDecimal.divide(new BigDecimal(size),10,BigDecimal.ROUND_HALF_DOWN);
////                double v1 = divide.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
//                    sumList.add(divide);
                }

            }
        }
//        int size = sumList.size();
//        BigDecimal sumDecimal=new BigDecimal(0);
//        for (BigDecimal decimal : sumList) {
//            sumDecimal=sumDecimal.add(decimal);
//        }
//        BigDecimal divide = sumDecimal.divide(new BigDecimal(size), 10, BigDecimal.ROUND_HALF_DOWN);
//        double v = divide.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        List<Future<Performance>> futures=new ArrayList<>();
        for (Map.Entry<String, List<JSONArray>> stringListEntry : indexMap.entrySet()) {
            String key = stringListEntry.getKey();
            List<JSONArray> value = stringListEntry.getValue();
            Task task = new Task(value, key);
            Future<Performance> future = executorService.submit(task);
            futures.add(future);
        }
        List<Performance> performances=new ArrayList<>();
        futures.forEach(performanceFuture -> {
            try {
                Performance performance = performanceFuture.get();
                performances.add(performance);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
        System.out.println(1);
    }


    public static class Task implements Callable<Performance> {

        private List<JSONArray> jsonArrays;
        private String key;
        public Task(List<JSONArray> jsonArrays,String key){
            this.jsonArrays=jsonArrays;
            this.key=key;
        }
        @Override
        public Performance call() throws Exception {
            BigDecimal sumDecimal=new BigDecimal(0);
            Long count=0l;
            for (JSONArray jsonArray : jsonArrays) {
                for (Object o : jsonArray) {
                    JSONObject data = (JSONObject) o;
                    BigDecimal value = (BigDecimal)data.get("value");
                    sumDecimal=sumDecimal.add(value);
                    count++;
                }
            }
            BigDecimal divide = sumDecimal.divide(new BigDecimal(count), 10, BigDecimal.ROUND_HALF_DOWN);
            Double v = divide.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();

            return new Performance(key,v);
        }
    }
}
