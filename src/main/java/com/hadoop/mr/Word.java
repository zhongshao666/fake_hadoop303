package com.hadoop.mr;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class Word extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Word(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/data/wordx.txt");//输入路径
        Path out = new Path("wordx");//输出路径

        Job job = Job.getInstance(conf, "jobName");
        job.setJarByClass(this.getClass());

        //设置本次的Mapper类为  ChainMapper
        //job.setMapperClass(ChainMapper.class);
        //给ChainMapper指定多个mapper类
        //ChainMapper.addMapper(job,map1.class,LongWritable.class,Text.class,Text.class,Text.class,conf);
        //指定分区
        //job.setPartitionerClass(?.class);
        //给Job设置以上自定义GroupingComparator
        //job.setGroupingComparatorClass(?.class);
        //reduce个数
        //job.setNumReduceTasks();
        //job.setCombinerClass(Class <? extends Reducer>);

        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);//map key2输出格式
        job.setMapOutputValueClass(Text.class);//map value2输出格式

        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(IntWritable.class);//reduce value3输出格式

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class WordMapper extends Mapper<LongWritable, Text, Text, Text> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("*"), value);
        }
    }

    static class WordReducer extends Reducer<Text, Text, Text, IntWritable> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> hashMap = new HashMap<>();
            HashMap<String, Integer> hashMapRes = new HashMap<>();
            for (Text text:
                 v2s) {
                String[] split = text.toString().split("\\s+");
                hashMap.put(split[0], Integer.valueOf(split[1]));
            }
            Set<Map.Entry<String, Integer>> entries = hashMap.entrySet();
            Set<Map.Entry<String, Integer>> entries1 = hashMap.entrySet();

            int x = 0;
            for (Map.Entry<String, Integer> map:
                 entries) {
                int y = 0;
                for (Map.Entry<String, Integer> map1:
                        entries1) {
                    if (x > y) {
                        y++;
                        continue;
                    }
                    int i = map.getValue() * map1.getValue();
                    String s = map.getKey() + "*" + map1.getKey();
                    hashMapRes.put(s, i);
                }
                x++;
            }
            Set<Map.Entry<String, Integer>> entries2 = hashMapRes.entrySet();
            for (Map.Entry<String, Integer> map:
                 entries2) {
                context.write(new Text(map.getKey()), new IntWritable(map.getValue()));
            }

        }
    }

}
