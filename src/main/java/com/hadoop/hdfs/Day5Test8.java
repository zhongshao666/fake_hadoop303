package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Day5Test8 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Writer.Option file = SequenceFile.Writer.file(new Path("/seq"));

        SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(Text.class);

        SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(Text.class);


        SequenceFile.Writer.Option compression = SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE);

        SequenceFile.Writer writer = SequenceFile.createWriter(conf, file,keyClass,valueClass,compression);

        writer.append(new Text("hhh1"),new Text("hello world 1!!!!"));
        writer.append(new Text("hhh2"),new Text("hello world 2!!!!"));
        writer.append(new Text("hhh3"),new Text("hello world 3!!!!"));

        writer.hflush();

        writer.close();

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Day5Test8(), args);
    }
}