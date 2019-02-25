package com.rimi.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class HbaseDemo {
    public static void main(String[] args) throws IOException {
        //创建配置
        Configuration conf = HBaseConfiguration.create();
        //获取连接
        Connection connection = ConnectionFactory.createConnection();

        //
        TableName tableName = TableName.valueOf("n1:t1");
        Table table = connection.getTable(tableName);

        //获取所有的数据i
        ResultScanner scanner = table.getScanner(new Scan());
       /* Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result next = iterator.next();
            byte[] value = next.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(Bytes.toString(value));
        }
*/
        Put put = new Put(Bytes.toBytes("row1"));

        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("name"),Bytes.toBytes("java hello！"));

        table.put(put);

        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result next = iterator.next();
            byte[] value = next.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(Bytes.toString(value));
        }

//        Delete delete = new Delete();


    }
}
