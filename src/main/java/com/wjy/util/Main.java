package com.wjy.util;

//import com.mongodb.MongoClient;
//import com.mongodb.ServerAddress;
//import com.mongodb.client.*;
//import com.mongodb.client.model.UpdateOneModel;
//import com.mongodb.client.model.WriteModel;
//import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {


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

    }
}
