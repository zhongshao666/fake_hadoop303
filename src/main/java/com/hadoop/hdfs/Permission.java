package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class Permission {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        FsPermission fsPermission = new FsPermission(FsAction.READ_EXECUTE, FsAction.EXECUTE, FsAction.NONE);
        fileSystem.setPermission(new Path("/a"),fsPermission);

    }
}
