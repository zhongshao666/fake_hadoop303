package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;


public class Append {



    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
//        conf.setBoolean("dfs.support.append", true);
/*        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);*/


        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream os = null;
        Path path=new Path("/b");

        //集群上的/a文件
        Path path1 = new Path("/a");

        // hdfs集群上  /a的输入流
        FSDataInputStream in = fs.open(path1);
        int len = 0;
        while ((len=in.read())!=-1){
            os=fs.append(path);
            os.write(len);
            os.flush();
            os.close();//每次追加完要关闭流
            System.out.write(len);
            System.out.flush();
        }

        in.close();

    }
}
