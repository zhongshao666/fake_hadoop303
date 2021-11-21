package com.hadoop.mr.day7work;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class Wea3 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Wea3(), args));
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
        Path out = new Path("./day6work3");//输出路径

        Job job = Job.getInstance(conf, "jobName");
        job.setJarByClass(this.getClass());

        job.setMapperClass(Wea3Mapper.class);
        job.setMapOutputKeyClass(Text.class);//map key2输出格式
        job.setMapOutputValueClass(Text.class);//map value2输出格式
        MultipleInputs.addInputPath(job, in1, TextInputFormat.class, Wea3Mapper.class);
        MultipleInputs.addInputPath(job, in2, TextInputFormat.class, Wea3Mapper.class);
        MultipleInputs.addInputPath(job, in3, TextInputFormat.class, Wea3Mapper.class);
        MultipleInputs.addInputPath(job, in4, TextInputFormat.class, Wea3Mapper.class);
        MultipleInputs.addInputPath(job, in5, TextInputFormat.class, Wea3Mapper.class);
        MultipleInputs.addInputPath(job, in6, TextInputFormat.class, Wea3Mapper.class);


        //指定分区
        //job.setPartitionerClass(?.class);
        //给Job设置以上自定义GroupingComparator
        //job.setGroupingComparatorClass(?.class);
        //reduce个数
        //job.setNumReduceTasks();

        job.setReducerClass(Wea3Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(FloatWritable.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class Wea3Mapper extends Mapper<LongWritable, Text, Text, Text> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            String str1 = string.substring(87, 92);
            if (!str1.equals("+9999")) {
                String substring2 = str1.substring(4, 5);
//                boolean matches = substring2.matches("[01459]");
                if (substring2.equals("0") || substring2.equals("1") || substring2.equals("4") || substring2.equals("5") || substring2.equals("9")) {
                    context.write(new Text(string.substring(15, 19)), new Text(str1.substring(0, 4)));
                }
            }
        }
    }

    static class Wea3Reducer extends Reducer<Text, Text, Text, FloatWritable> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (Text i :
                    v2s) {
                sum += Float.parseFloat(i.toString());
                count++;
            }
            String k3 = k2.toString() + "\t" + count + "条";
            context.write(new Text(k3), new FloatWritable(sum / count));
        }
    }

}
