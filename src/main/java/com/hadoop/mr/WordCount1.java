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
import java.util.StringTokenizer;

public class WordCount1 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCount1(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in = new Path("/word.txt");
        Path out=new Path("/word1");

        Job job = Job.getInstance(conf, "单词计数");
        job.setJarByClass(this.getClass());


        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;

    }
    static class WordCountMapper extends Mapper <LongWritable,Text,Text,IntWritable> {

        private final static IntWritable one=new IntWritable(1); //统计使用变量
        private Text word=new Text(); //单词变量

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer words=new StringTokenizer(value.toString());
            while(words.hasMoreTokens()){
                word.set(words.nextToken());
                context.write(word, one);
            }
        }
    }

    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text k3=new Text();
        private IntWritable v3=new IntWritable();

        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context) throws IOException, InterruptedException {
            this.k3.set(k2.toString());

            int sum=0;
            for(IntWritable v2: v2s){
                sum+=v2.get();
            }
            this.v3.set(sum);


            context.write(this.k3,this.v3);
        }
    }


}

/*

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
import java.util.StringTokenizer;

public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCount(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in = new Path("/data/word.txt");
        Path out=new Path("./word1");

        Job job = Job.getInstance(conf, "单词计数");
        job.setJarByClass(this.getClass());


        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;

    }
    static class WordCountMapper extends Mapper <LongWritable,Text,Text,IntWritable> {

        private final static IntWritable one=new IntWritable(1); //统计使用变量
        private Text word=new Text(); //单词变量

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer words=new StringTokenizer(value.toString());
            while(words.hasMoreTokens()){
                word.set(words.nextToken());
                context.write(word, one);
            }
        }
    }

    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text k3=new Text();
        private IntWritable v3=new IntWritable();

        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context) throws IOException, InterruptedException {
            this.k3.set(k2.toString());

            int sum=0;
            for(IntWritable v2: v2s){
                sum+=v2.get();
            }
            this.v3.set(sum);

            context.write(this.k3,this.v3);
        }
    }


}



**/
