package com.wjq.jdbc;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

public class MongoUtil {
    public static void main(String[] args) {
        getMongoConn();
    }
    public static void getMongoConn(){
        String mongo_uri = "mongodb://127.0.0.1:27017/test";
        MongoClientURI mongoClientUri =new MongoClientURI(mongo_uri);
        MongoClient mongoClient = new MongoClient(mongoClientUri);

        // 获取mongo中的所有数据库
        ListDatabasesIterable<Document> documents = mongoClient.listDatabases();

        for (Document document : documents) {
            System.out.println(document.toString());
        }
//        // 使用test数据库
//        MongoDatabase testdb = mongoClient.getDatabase("test");
//        // 获取user文档
//        MongoCollection<Document> user = testdb.getCollection("user");
//        // 对user文档进行查询
//        FindIterable<Document> documents1 = user.find();
//        for (Document document : documents1) {
//            System.out.println(document);
//        }
//        System.out.println("-----------------------------------------------------------");
//        // 创建集合
////        testdb.createCollection("testJava");
//        // 插入文档
//        MongoCollection<Document> testJava = testdb.getCollection("testJava");
//        Document document = new Document("sno", 2015103)
//                .append("name", "张三")
//                .append("age", 12)
//                .append("sex" , "male");
//
//        testJava.insertOne(document);
//
//        FindIterable<Document> documents2 = testJava.find();
//
//        for (Document document1 : documents2) {
//            System.out.println(document1);
//        }
//
//        // 删除文档
//
////        DeleteResult age = testJava.deleteMany(Filters.eq("age", 12));
////        System.out.println(age.wasAcknowledged());
//        // 修改文档
//        testJava.updateMany(Filters.eq("age",13), new Document("$set",new Document("age",88)));
    }
}
