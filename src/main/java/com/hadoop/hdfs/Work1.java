package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;

public class Work1 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        test1(conf);
        return 0;
    }

    static void test2(Configuration conf)throws Exception{
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataOutputStreamBuilder builder = fileSystem.appendFile(new Path("/day3test/a"));
        FSDataOutputStream build = builder.build();
        build.write("zzh\n".getBytes());
        build.flush();
        build.close();

    }

    static void test1(Configuration conf)throws Exception{
        FileSystem fs = FileSystem.get(conf);

        FileChecksum fileChecksum = fs.getFileChecksum(new Path("/a"));
        System.out.println("fileChecksum = " + fileChecksum);

        FSDataInputStream open = fs.open(new Path("/b"));
        HdfsDataInputStream is = (HdfsDataInputStream) open;
        List<LocatedBlock> allBlocks = is.getAllBlocks();
        allBlocks.forEach(System.out::println);

        FileSystem checksummedFs=new ChecksumFileSystem(fs){};
        FileStatus[] fes = checksummedFs.listStatus(new Path("/day3test/a"));
        for (FileStatus f:
                fes) {

            System.out.println("文件路径："+f.getPath());
            System.out.println("块的大小："+f.getBlockSize());
            System.out.println("文件所有者："+f.getOwner()+":"+f.getGroup());
            System.out.println("文件权限："+f.getPermission());
            System.out.println("文件长度："+f.getLen());
            System.out.println("备份数："+f.getReplication());
            System.out.println("修改时间："+f.getModificationTime());
        }

        //f对象封装了文件的和目录的额元数据，包括文件长度、块大小、权限等信息
/*        f f = fs.getf(new Path("/day3test/a"));
        System.out.println("文件路径："+f.getPath());
        System.out.println("块的大小："+f.getBlockSize());
        System.out.println("文件所有者："+f.getOwner()+":"+f.getGroup());
        System.out.println("文件权限："+f.getPermission());
        System.out.println("文件长度："+f.getLen());
        System.out.println("备份数："+f.getReplication());
        System.out.println("修改时间："+f.getModificationTime());*/
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Work1(), args);
    }
}