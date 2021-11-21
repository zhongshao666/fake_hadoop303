package com.hadoop;
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

public class WordCount extends Configured implements Tool {

    //                                          前两个是输入          输出
    static class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            //import org.apache.hadoop.io.Text;  ->  import org apache hadoop io Text
            String s = string.replaceAll("[.;,?:]", " ");
            //\\s+   空格  \t  多个空格
            String[] split = s.split("\\s+");
            //import org apache hadoop io Text ->  split[import,org,apache,hadoop,io,Text]
            for (int i = 0; i <split.length ; i++) {
                //提交给reduce   豆角
                context.write(new Text(split[i]),new IntWritable(1));
            }
        }
    }

    static class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable ignored:
                 values) {
                count++;
            }
            context.write(key,new IntWritable(count));
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "wod6666");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("TimeUtil.java"));

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("java"));

        return job.waitForCompletion(true)?0:1;

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCount(),args));
    }
}
