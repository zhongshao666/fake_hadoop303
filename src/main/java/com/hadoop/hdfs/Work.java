package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.util.List;

public class Work {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
//        de(fileSystem);
        test2(fileSystem);
    }

    private static void de(FileSystem fileSystem) throws Exception {
        fileSystem.delete(new Path("/a"), true);
    }

    private static void append(FileSystem fileSystem) throws Exception {
        Path path = new Path("/a");
        Path path1 = new Path("/b");

        FSDataOutputStreamBuilder builder = fileSystem.appendFile(path);

        FSDataOutputStream os = builder.build();

        os.write("hhhh".getBytes());
        os.flush();
        os.close();
    }

    private static void chmod(FileSystem fileSystem) throws Exception {

        FsPermission fsPermission = new FsPermission(FsAction.READ_EXECUTE, FsAction.EXECUTE, FsAction.NONE);

//        FSDataOutputStream fsDataOutputStream = new FSDataOutputStream((short) 777);
        FsPermission aDefault = FsPermission.getDefault();
        //new FsPermission((short)00777)
        FsPermission fsPermission1 = new FsPermission((short) 00644);


        fileSystem.setPermission(new Path("/a"), fsPermission1);

    }

    private static void test1(FileSystem fileSystem, Path path) throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(path);

        for (FileStatus fileStatus :
                fileStatuses) {
            boolean file = fileStatus.isFile();
            if (file) {
                System.out.println("文件：" + fileStatus.getPath());
            }

            if (fileStatus.isDirectory()) {
                System.out.println("文件夹：" + fileStatus.getPath());
                test1(fileSystem, fileStatus.getPath());
            }
        }

    }

    public static void test2(FileSystem fs)throws Exception {


        FSDataInputStream in =
                fs.open(new Path("/a"));
        //把输入流强制转换为 HdfsDataInputStream,类中有数据块信息
        HdfsDataInputStream hdis =
                (HdfsDataInputStream) in;
        //获取到 open打开文件的 所有数据块信息
        List<LocatedBlock> allBlocks =
                hdis.getAllBlocks();
        for (LocatedBlock l : allBlocks) {
            ExtendedBlock block = l.getBlock();
            System.out.println(block.getBlockName()
                    + "--" + block.getBlockId());
        }
    }

}
