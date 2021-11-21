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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;


public class mr2_2_2 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new mr2_2_2(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("day8work2_1");//输入路径
        Path out = new Path("day8work2_2");//输出路径

        Job job = Job.getInstance(conf, "class2");
        job.setJarByClass(this.getClass());

        job.setMapperClass(mr2_2_2Mapper.class);
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

        job.setReducerClass(mr2_2_2Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(IntWritable.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class mr2_2_2Mapper extends Mapper<LongWritable, Text, Text, Text> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            String[] split = string.split("\\s+")[1].split(",");
//            ArrayList<String> list = new ArrayList<>(Arrays.asList(split));
//            Collections.sort(list);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i <split.length-1 ; i++) {
                for (int j = i+1; j <split.length ; j++) {
                    sb.append(split[i]).append(",").append(split[j]);
                    context.write(new Text(sb.toString()), new Text(""));
                    sb.delete(0,sb.length());
                }
            }
/*            for (int i = 0; i <list.size()-1 ; i++) {
                for (int j = i+1; j <list.size() ; j++) {
                    sb.append(list.get(i)).append(",").append(list.get(j));
                    context.write(new Text(sb.toString()), new Text(""));
                    sb.delete(0,sb.length());
                }
            }*/

        }
    }

    static class mr2_2_2Reducer extends Reducer<Text, Text, Text, IntWritable> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text ignored:
                 v2s) {
                sum++;
            }
            context.write(new Text(k2), new IntWritable(sum));
        }
    }

}
