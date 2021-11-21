package com.hbase.day1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public class FirstHbase {

    public static void main(String[] args) throws Exception {

        //1.获取HBaseConfiguration
        Configuration conf = HBaseConfiguration.create();
        //hbase集群zookeeper地址
        conf.set("hbase.zookeeper.quorum", "fake:2181");
        //2.获取连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        //3.获取Admin(DDL) 或者 Table(DML)
        Admin admin = conn.getAdmin();
        //4.操作代码
        //自定义命名空间
        NamespaceDescriptor nc = NamespaceDescriptor.create("bd05").build();
        admin.createNamespace(nc);
        //获取所有命名空间

        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        System.out.println(Arrays.toString(namespaceDescriptors));
        //5.关闭资源
        admin.close();
        conn.close();
    }
}
