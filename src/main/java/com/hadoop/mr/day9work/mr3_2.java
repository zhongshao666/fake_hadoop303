package com.hadoop.mr.day9work;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class mr3_2 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new mr3_2(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/data/mr3/good.txt");//输入路径
        Path out = new Path("mr3_2");//输出路径

        Job job = Job.getInstance(conf, "good");
        job.setJarByClass(this.getClass());

        job.setMapperClass(ChainMapper.class);
        ChainMapper.addMapper(job,mr3_2Mapper1.class,LongWritable.class,Text.class,Text.class,Text.class,conf);
        ChainMapper.addMapper(job,mr3_2Mapper2.class,Text.class,Text.class,Text.class,Text.class,conf);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);

        //指定分区
        //job.setPartitionerClass(?.class);
        //给Job设置以上自定义GroupingComparator
        //job.setGroupingComparatorClass(?.class);
        //reduce个数
        job.setNumReduceTasks(0);
        //job.setCombinerClass(Class <? extends Reducer>);

/*        job.setReducerClass(mr3_2Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(Text.class);//reduce value3输出格式*/
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class mr3_2Mapper1 extends Mapper<LongWritable, Text, Text, Text> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\s+");
            int price = Integer.parseInt(split[1]);
            if (price > 1000 && price < 3000) {
                context.write(value, new Text(""));
            }
        }
    }

    static class mr3_2Mapper2 extends Mapper<Text, Text, Text, Text> {//map输出格式


        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = key.toString().split("\\s+");
            if(split[2].equals("服装"))
                context.write(key, new Text(""));
        }
    }

/*    static class mr3_2Reducer extends Reducer<Text, Text, Text, Text> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {

            context.write(new Text(), new Text());
        }
    }*/

}
