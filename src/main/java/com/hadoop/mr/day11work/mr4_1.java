package com.hadoop.mr.day11work;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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


public class mr4_1 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new mr4_1(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/data/mr4");//输入路径
        Path out = new Path("mr4_1");//输出路径

        Job job = Job.getInstance(conf, "mr4_1");
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

        job.setMapperClass(mr4_1Mapper.class);
        job.setMapOutputKeyClass(Bean.class);//map key2输出格式
        job.setMapOutputValueClass(Text.class);//map value2输出格式

        job.setReducerClass(mr4_1Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(Text.class);//reduce value3输出格式

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class mr4_1Mapper extends Mapper<LongWritable, Text, Bean, Text> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\s+");
            Bean bean = new Bean(new Text(split[0]), new IntWritable(Integer.parseInt(split[1])), new FloatWritable(Float.parseFloat(split[2])));
            context.write(bean, new Text());
        }
    }

    static class mr4_1Reducer extends Reducer<Bean, Text, Text, Text> {//reduce输入输出格式


        @Override
        protected void reduce(Bean k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            context.write(new Text(k2.getName().toString() + "\t" + k2.getId().get() + "\t" + k2.getScore()), new Text());
        }
    }

}
