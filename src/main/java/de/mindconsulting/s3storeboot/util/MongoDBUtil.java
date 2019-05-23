package de.mindconsulting.s3storeboot.util;

import com.mongodb.MongoClient;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoDBUtil {

    //链接mongoDB数据库
//    public MongoDatabase getConnect(){
//        //连接到 mongodb 服务
//        MongoClient mongoClient = new MongoClient("152.136.11.202", 27017);
//
//        //连接到数据库
//        MongoDatabase mongoDatabase = mongoClient.getDatabase("test");
//
//        //返回连接数据库对象
//        return mongoDatabase;
//    }

    /**
     * 获取指定数据库下的所有集合
     * 使用：com.mongodb.client.MongoDatabase#listCollections()
     *
     * @param databaseName 数据库名称
     * @return 返回集合的 Document 对象，此对象包含集合的完整信息

     */
    public List<Document> getAllCollectionName(String databaseName) {

        List<Document> collectionList = new ArrayList<Document>();

        if(databaseName != null && "".equals(databaseName)) {
            MongoClient mongoClient = new MongoClient("152.136.11.202", 27017);

            //连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("test");

            ListCollectionsIterable<Document> collectionsIterable = mongoDatabase.listCollections();

            Document firstDocument = collectionsIterable.first();

            if(firstDocument == null) {
                return null;
            }

            MongoCursor<Document> mongoCursor = collectionsIterable.iterator();

            while (mongoCursor.hasNext()) {

                Document document = mongoCursor.next();
                collectionList.add(document);
            }
            mongoCursor.close();
            mongoClient.close();
        }

        return collectionList;
    }




    /**
     * 删除指定数据库下的指定集合，如果数据库中不存在此集合，则不会做任何处理
     * 使用：com.mongodb.client.MongoCollection#drop()
     *
     * @param databaseName   数据库名称
     * @param collectionName 获取的集合名称
     */
    public static void delCollection(String databaseName, String collectionName) {
        if (databaseName != null && !"".equals(databaseName) && collectionName != null && !"".equals(collectionName)) {
            /** MongoClient(String host, int port)：直接指定 MongoDB IP 与端口进行连接
             *  实际应用中应该将 MongoDB 服务端地址配置在 配置文件中*/
            MongoClient mongoClient = new MongoClient("152.136.11.202", 27017);

            /**getDatabase(String databaseName)：获取指定的数据库
             * 如果此数据库不存在，则会自动创建，此时存在内存中，服务器不会存在真实的数据库文件，show dbs 命令 看不到
             * 如果再往其中添加数据，服务器则会生成数据库文件，磁盘中会真实存在，show dbs 命令 可以看到
             * */
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);

            /**获取数据库中的集合*/
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);

            /**删除当前集合，如果集合不存在，则不做任何处理，不会抛异常*/
            mongoCollection.drop();
            mongoClient.close();
        }
    }


    /**
     * 显示的为指定数据库创建集合
     *
     * @param databaseName   数据库名称，如 java，不存在时会自动创建，存在时不受影响
     * @param collectionName 集合名词，如 c1，不存在时会自动创建，存在时则会抛出异常： already exists'
     */
    public static void createCollectionByShow(String databaseName, String collectionName) {
        if (databaseName != null && !"".equals(databaseName) && collectionName != null && !"".equals(collectionName)) {
            /** MongoClient(String host, int port)：直接指定 MongoDB IP 与端口进行连接
             * 实际应用中应该将 MongoDB 服务端地址配置在 配置文件中*/
            MongoClient mongoClient = new MongoClient("152.136.11.202", 27017);

            /**getDatabase(String databaseName)：获取指定的数据库
             * 如果此数据库不存在，则会自动创建，此时存在内存中，服务器不会存在真实的数据库文件，show dbs 命令 看不到
             * 如果再往其中添加数据，服务器则会生成数据库文件，磁盘中会真实存在，show dbs 命令 可以看到
             * */
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);

            /**createCollection(String var1)：显示创建集合，此时 java 数据库下会立即创建 c1 集合
             * 注意如果 数据库中已经存在此 集合，则会抛出异常： already exists'
             *
             * 执行完成后，MongoDB  客户端可以用命令查看：
             * > show dbs
             * admin   0.000GB
             * config  0.000GB
             * java    0.000GB
             * local   0.000GB
             * > use java
             * switched to db java
             * > show tables
             * c1
             * */
            mongoDatabase.createCollection(collectionName);

            /**关闭 MongoDB 客户端连接，释放资源*/
            mongoClient.close();
        }
    }


    /**
     * 获取指定数据库下的指定集合
     *
     * @param databaseName   数据库名称，不存在时，MongoCollection 大小为 0
     * @param collectionName 获取的集合名称,不存在时，MongoCollection 大小为 0
     */
    public  MongoCollection<Document> getCollectionByName(String databaseName, String collectionName) {
        MongoCollection<Document> mongoCollection = null;
        if (databaseName != null && !"".equals(databaseName) && collectionName != null && !"".equals(collectionName)) {
            /** MongoClient(String host, int port)：直接指定 MongoDB IP 与端口进行连接
             * 实际应用中应该将 MongoDB 服务端地址配置在 配置文件中*/
            MongoClient mongoClient = new MongoClient("152.136.11.202", 27017);

            /**getDatabase(String databaseName)：获取指定的数据库
             * 如果此数据库不存在，则会自动创建，此时存在内存中，服务器不会存在真实的数据库文件，show dbs 命令 看不到
             * 如果再往其中添加数据，服务器则会生成数据库文件，磁盘中会真实存在，show dbs 命令 可以看到
             * */
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);

            /**获取数据库中的集合
             * 如果集合不存在，则返回的 MongoCollection<Document> 文档个数为0，不会为 null*/
            mongoCollection = mongoDatabase.getCollection(collectionName);
        }
        return mongoCollection;
    }





}
