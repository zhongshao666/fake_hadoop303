package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileOutputStream;
import java.net.URI;
import java.util.List;
/**
 * 校验和用法:<br>
 *     上传文件,可以自己用代码控制,<br>
 *     持久化保存,默认,不能控制<br>
 *
 *       LocalFileSystem : 表示linux本地文件系统<Br>
 *           处理校验和
 *           lfs可以获取 Out :
 *              把代码中的数据输出到linux本地,
 *              会顺带输出该数据对应的校验和
 *           lfs可以获取 in  :
 *              从linux本地读取数据到代码中,
 *              会检验 数据和校验和的对应关系
 *       RawLocalFileSystem : 表示linux本地文件系统<Br>
 *           不处理校验和
 * */
public class Test4 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Test4(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        t2(conf);
        return 0;
    }
    /**
     * 不使用校验和读取数据
     * RawLocalFileSystem
     * */
    public void t3(Configuration conf)throws  Exception{
        //新建的RawLFS需要初始化
        RawLocalFileSystem rlfs = new RawLocalFileSystem();
        URI uri = URI.create("/home/zzh/aabb.txt");
        rlfs.initialize(uri,conf);

        //获取输入输出流,本次的输入流不具备检验校验和功能
        FSDataInputStream in =
                rlfs.open(new Path("/home/zzh/aabb.txt"));
        IOUtils.copyBytes(in,System.out,100,true);

    }

    /**
     * 使用lfs读取linux本地的数据
     * */
    public void t2( Configuration conf)throws  Exception{
        LocalFileSystem lfs = LocalFileSystem.getLocal(conf);

        FSDataInputStream in = lfs.open(new Path("/home/zzh/yyhh.txt"));

        IOUtils.copyBytes(in,System.out,100,true);

    }
    /**
     * 写数据到linux本地,有校验和
     * LocalFileSystem lfs
     * */
    public void t1( Configuration conf)throws  Exception{
        //操作本地linux的文件系统对象
        LocalFileSystem lfs =
                LocalFileSystem.getLocal(conf);
        //使用本地操作系统对象,
        // 调用输入输出流方法,
        // 进行读取本地数据和写数据到本地的操作
        Path p = new Path("/home/zzh/yyhh.txt");
        FSDataOutputStream out = lfs.create(p);

        out.write("hhh".getBytes());
        out.write("world".getBytes());
        out.flush();
        out.close();

    }
}
