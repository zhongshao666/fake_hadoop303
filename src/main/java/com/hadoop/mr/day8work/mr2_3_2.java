package com.hadoop.mr.day8work;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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


public class mr2_3_2 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new mr2_3_2(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in1 = new Path("/data/mr2_1_1/data3/fruit.txt");//输入路径
        Path in2 = new Path("day8work3_1");//输入路径
        Path out = new Path("day8work3_2");//输出路径

        Job job = Job.getInstance(conf, "fruit");
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(job,in1,TextInputFormat.class,mr2_3_2Mapper1.class);
        MultipleInputs.addInputPath(job,in2,TextInputFormat.class,mr2_3_2Mapper2.class);


        job.setMapOutputKeyClass(Text.class);//map key2输出格式
        job.setMapOutputValueClass(DoubleWritable.class);//map value2输出格式


        //指定分区
        //job.setPartitionerClass(?.class);
        //给Job设置以上自定义GroupingComparator
        //job.setGroupingComparatorClass(?.class);
        //reduce个数
        //job.setNumReduceTasks();

        job.setReducerClass(mr2_3_2Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(DoubleWritable.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class mr2_3_2Mapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\s+");
            String[] split1 = split[1].split("[/]");
            double price = Double.parseDouble(split1[1]) / Double.parseDouble(split1[0]);
            context.write(new Text(split[0]), new DoubleWritable(price));
        }
    }

    static class mr2_3_2Mapper2 extends Mapper<LongWritable, Text, Text, DoubleWritable> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\s+");
            context.write(new Text(split[0]), new DoubleWritable(Double.parseDouble(split[1])));
        }
    }

    static class mr2_3_2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<DoubleWritable> v2s, Context context) throws IOException, InterruptedException {
            double sum;
            double[] p = new double[2];
            int i = 0;
            for (DoubleWritable s:
                 v2s) {
                p[i] = s.get();
                i++;
            }
            sum = p[0] * p[1];
            context.write(new Text(k2), new DoubleWritable(sum));
        }
    }

}
