package com.hadoop.mr.day7work;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class Wea1 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Wea1(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in1 = new Path("/data/029070-99999-1901");//输入路径
        Path in2 = new Path("/data/029070-99999-1902");//输入路径
        Path in3 = new Path("/data/029070-99999-1903");//输入路径
        Path in4 = new Path("/data/029500-99999-1901");//输入路径
        Path in5 = new Path("/data/029500-99999-1902");//输入路径
        Path in6 = new Path("/data/029500-99999-1903");//输入路径
        Path out = new Path("./day6work1");//输出路径

        Job job = Job.getInstance(conf, "Wea1");
        job.setJarByClass(this.getClass());

        job.setMapperClass(Wea1Mapper.class);
        job.setMapOutputKeyClass(Text.class);//map key2输出格式
        job.setMapOutputValueClass(Text.class);//map value2输出格式
        MultipleInputs.addInputPath(job,in1,TextInputFormat.class,Wea1Mapper.class);
        MultipleInputs.addInputPath(job,in2,TextInputFormat.class,Wea1Mapper.class);
        MultipleInputs.addInputPath(job,in3,TextInputFormat.class,Wea1Mapper.class);
        MultipleInputs.addInputPath(job,in4,TextInputFormat.class,Wea1Mapper.class);
        MultipleInputs.addInputPath(job,in5,TextInputFormat.class,Wea1Mapper.class);
        MultipleInputs.addInputPath(job,in6,TextInputFormat.class,Wea1Mapper.class);

        //指定分区
        //job.setPartitionerClass(?.class);
        //给Job设置以上自定义GroupingComparator
        //job.setGroupingComparatorClass(?.class);
        //reduce个数
        //job.setNumReduceTasks();

        job.setReducerClass(Wea1Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(IntWritable.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class Wea1Mapper extends Mapper<LongWritable, Text, Text, Text> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            String str1 = string.substring(87, 92);
            if (!str1.equals("+9999")) {
                String substring2 = str1.substring(4, 5);
//                boolean matches = substring2.matches("[01459]");
                if (substring2.equals("0") || substring2.equals("1") || substring2.equals("4") || substring2.equals("5") || substring2.equals("9")) {
                    context.write(new Text(string.substring(0,15)), new Text(str1.substring(0, 4)));
                }
            }
        }
    }

    static class Wea1Reducer extends Reducer<Text, Text, Text, IntWritable> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            String t;
            int temp;
            for (Text text : v2s) {
                t = text.toString();
                temp = Integer.parseInt(t);
                if (temp > max)
                    max = temp;
            }
            context.write(new Text(k2), new IntWritable(max));
        }
    }

}
