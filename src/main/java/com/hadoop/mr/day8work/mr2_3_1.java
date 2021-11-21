package com.hadoop.mr.day8work;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class mr2_3_1 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new mr2_3_1(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/data/mr2_1_1/data3/order.txt");//输入路径
        Path out = new Path("day8work3_1");//输出路径

        Job job = Job.getInstance(conf, "order");
        job.setJarByClass(this.getClass());

        job.setMapperClass(mr2_3_1Mapper.class);
        job.setMapOutputKeyClass(Text.class);//map key2输出格式
        job.setMapOutputValueClass(IntWritable.class);//map value2输出格式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);

        //指定分区
        //job.setPartitionerClass(?.class);
        //给Job设置以上自定义GroupingComparator
        //job.setGroupingComparatorClass(?.class);
        //reduce个数
        //job.setNumReduceTasks();

        job.setReducerClass(mr2_3_1Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(IntWritable.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class mr2_3_1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\s+");
            String s = split[1].split("[+]")[1];
            context.write(new Text(split[0]), new IntWritable(Integer.parseInt(s)));
        }
    }

    static class mr2_3_1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i:
                 v2s) {
                sum += i.get();
            }
            context.write(k2, new IntWritable(sum));
        }
    }

}
