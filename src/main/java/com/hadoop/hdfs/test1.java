package com.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class test1 {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //设置相关参数
        //该参数也可以不设置，需要将集群中四个xml配置文件拷贝到项目resources源里
        //configuration.set("fs.defaultFS","hdfs://fake:9000");

        FileSystem fs = FileSystem.get(conf);

        //集群上的/a文件
        Path p1 = new Path("/a");

        // hdfs集群上  /a的输入流
        FSDataInputStream in = fs.open(p1);
        int len = 0;
        while ((len=in.read())!=-1){
            System.out.write(len);
            System.out.flush();
        }

        in.close();
        fs.close();

    }
}
