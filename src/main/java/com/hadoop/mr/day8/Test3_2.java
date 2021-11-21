package com.hadoop.mr.day8;

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


public class Test3_2 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Test3_2(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("./day8test3_1");//输入路径
        Path out = new Path("./day8test3_2");//输出路径

        Job job = Job.getInstance(conf, "jobName");
        job.setJarByClass(this.getClass());

        job.setMapperClass(Test3_2Mapper.class);
        job.setMapOutputKeyClass(Text.class);//map key2输出格式
        job.setMapOutputValueClass(Text.class);//map value2输出格式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);

        //指定分区
        //job.setPartitionerClass(?.class);
        //给Job设置以上自定义GroupingComparator
        //job.setGroupingComparatorClass(?.class);
        //reduce个数
        //job.setNumReduceTasks();

        job.setReducerClass(Test3_2Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(IntWritable.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class Test3_2Mapper extends Mapper<LongWritable, Text, Text, Text> {//map输出格式


        //共现
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            String[] split = string.split("\t")[1].split(",");
            for (int i = 0; i <split.length-1 ; i++) {
                for (int j = i+1; j <split.length ; j++) {
                    String endKey = split[i].trim() + "," + split[j].trim();
                    context.write(new Text(endKey), new Text(""));
                }
            }
        }
    }

    static class Test3_2Reducer extends Reducer<Text, Text, Text, IntWritable> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text ignored :
                 v2s) {
                sum++;
            }
            context.write(k2, new IntWritable(sum));
        }
    }

}
