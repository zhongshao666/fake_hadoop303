package com.hadoop.hdfs;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.Arrays;

public class Work2 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        Bean t = new Bean(new IntWritable(1),new Text("tomm"));
        t.write(new DataOutputStream(b));
        //对象在buf字节数组中
        byte[] buf = b.toByteArray();
        System.out.println(Arrays.toString(buf));

        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        DataInputStream dis = new DataInputStream(bais);
        Bean tt = new Bean();
        tt.readFields(dis);

        System.out.println(tt);


        return 0;
    }

    public static void work1(Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        LocalFileSystem lfs = LocalFileSystem.getLocal(conf);
        //新建的RawLFS需要初始化
/*        RawLocalFileSystem rlfs = new RawLocalFileSystem();
        URI uri = URI.create("/home/zzh/aabb.txt");
        rlfs.initialize(uri,conf);*/

        //读取linux本地
        FSDataInputStream is = lfs.open(new Path("/home/zzh/a.jpg"));
        //写入hdfs集群
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/a.gz"));
        //获取编解码工厂后获取编解码器
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec gzip = factory.getCodecByName("gzip");
        //具有编解码功能的流
        CompressionOutputStream outputStream = gzip.createOutputStream(fsDataOutputStream);

//        IOUtils.copyBytes(inputStream,fsDataOutputStream,100,true);
//        fsDataOutputStream.flush();

        int len = 0;
        while ((len = is.read()) != -1) {
            outputStream.write(len);
            outputStream.flush();
        }


        outputStream.close();
        fsDataOutputStream.close();
        is.close();
        fs.close();
        lfs.close();
    }

    public static void work2(Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        LocalFileSystem lfs = LocalFileSystem.getLocal(conf);
        //写入本地输出流
        FSDataOutputStream fsDataOutputStream = lfs.create(new Path("/home/zzh/a1.jpg"));

        FSDataInputStream fsDataInputStream = fs.open(new Path("/a.gz"));

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec gzip = factory.getCodecByName("gzip");
        CompressionInputStream inputStream = gzip.createInputStream(fsDataInputStream);

        IOUtils.copyBytes(inputStream, fsDataOutputStream, 1024, true);

        fsDataOutputStream.flush();
        fsDataOutputStream.close();
        inputStream.close();
        fsDataInputStream.close();
        lfs.close();
        fs.close();


    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Work2(), args);
    }

}

class Bean implements WritableComparable {

    private IntWritable id;
    private Text name;

    public Bean() {
    }

    public Bean(IntWritable id, Text name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Bean{" +
                "id=" + id +
                ", name=" + name +
                '}';
    }

    public IntWritable getId() {
        return id;
    }

    public void setId(IntWritable id) {
        this.id = id;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id.get());
        out.writeUTF(name.toString());

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = new IntWritable(in.readInt());
        name = new Text(in.readUTF());
    }

}