package com.wjy.util;

import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

@Data
public class HbaseUtil {
    private static HbaseUtil instance = null;
    public  static synchronized HbaseUtil getInstance() {
        if(null == instance) {
            instance = new HbaseUtil();
        }
        return instance;
    }
    private Connection connection = null;
    private Admin admin = null;
    private Configuration configuration=null;

    private HbaseUtil(){
        configuration=new Configuration();
        configuration.set("hbase.zookeeper.quorum","wang-109,wang-208,wang-209");
        configuration.set("hbase.rootdir","hdfs://mycluster/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin=connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Table getTable(String tableName){
        Table table=null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public void put(String tableName, String rowkey, String cf, String column, String value) {
        Table table = getTable(tableName);
        Put put=new Put(Bytes.toBytes(tableName));
        put.addColumn(cf.getBytes(),column.getBytes(),value.getBytes());
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

