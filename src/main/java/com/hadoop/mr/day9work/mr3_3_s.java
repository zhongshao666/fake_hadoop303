package com.hadoop.mr.day9work;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.net.URI;



public class mr3_3_s extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new mr3_3_s(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "hhhhhh");
        job.setJarByClass(this.getClass());

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        //设置多个Reduce
        job.setNumReduceTasks(2);

        //指定分区比较器
        job.setPartitionerClass(TotalOrderPartitioner.class);


        //获取文件的uri
        String partitionFile =
                TotalOrderPartitioner.getPartitionFile(conf);
        URI uri = new URI(partitionFile);

        //自定义比较器
        job.setSortComparatorClass(MyIntWritable.class);

        System.out.println(uri.toString());
        //任务就能识别到文件了，把文件就可以分发到节点上。
        job.addCacheFile(uri);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        //在我的hdfs上/seq_4/part-r-00000是一个SequenceFile文件
        SequenceFileInputFormat.addInputPath(
                job, new Path("mr3_3_1"));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("mr3_3_2"));

        //采样器
        InputSampler.RandomSampler<Text, Text> r =
                new InputSampler.RandomSampler<Text,Text>(
                        //采样比例   采样数量上限      分界点个数
                        0.5, 10, 1);
        InputSampler.writePartitionFile(job, r);

        job.waitForCompletion(true);
        return 0;

    }

    /**
     * mapper<br>
     * 准备读取SequenceFIle文件,
     * 准备读取的SequenceFile文件key是Text类型,value是Text类型
     */
    public static class MyMapper
            extends Mapper<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class MyReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
/*            StringBuilder sb = new StringBuilder();
            for (Text text:
                 values) {
                sb.append(text.toString()).append(",");
            }
            sb.delete(sb.length() - 1, sb.length());*/
            context.write(key, values.iterator().next());
        }
    }


}

class MyIntWritable extends IntWritable.Comparator{
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return -super.compare(b1, s1, l1, b2, s2, l2);
    }
}
