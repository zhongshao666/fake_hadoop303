package com.hadoop.mr.day6;


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
import java.util.StringTokenizer;

public class WordCount extends Configured implements Tool  {


    static class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override

        /**
        * @Description  TODO

        * @return void
        **/
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String msg = value.toString();
//            msg = msg.replaceAll("[.'\"!?:,;]", "");

            StringTokenizer words=new StringTokenizer(value.toString());
            while(words.hasMoreTokens()){
                context.write(new Text(words.nextToken()), new IntWritable(1));
            }
//            String[] split = msg.split("");
//            for (String k:words) {
//                context.write(new Text(k),new IntWritable(1));
//            }
        }
    }

    static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable ignored :
                 values) {
                sum++;
            }
            context.write(key,new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
//        String n = conf.get("n");

        //Job
        Job job = Job.getInstance(conf, "6666");
        job.setJarByClass(this.getClass());

        //map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定map输入输出类型和路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/word.txt"));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/word_1"));




        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCount(),args));
    }


}
