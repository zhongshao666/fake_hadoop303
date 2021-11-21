package com.hadoop.mr.day8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.Collection;
import java.util.Collections;

//好友共现
public class Test3_1 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Test3_1(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/data/5_共现矩阵/friends.txt");//输入路径
        Path out = new Path("./day8test3_1");//输出路径

        Job job = Job.getInstance(conf, "jobName");
        job.setJarByClass(this.getClass());

        job.setMapperClass(Test3Mapper.class);
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

        job.setReducerClass(Test3Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(Text.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class Test3Mapper extends Mapper<LongWritable, Text, Text, Text> {//map输出格式


        /**
         * @Description TODO
         * joe,bob
         * tom,lili
         **/
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            String[] split = string.split(",");
            context.write(new Text(split[0].trim()), new Text(split[1].trim()));
        }
    }

    static class Test3Reducer extends Reducer<Text, Text, Text, Text> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            ArrayList<String> list = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            for (Text text:
                 v2s) {
                list.add(text.toString());
            }
            Collections.sort(list);
            for (String s:
                 list) {
                sb.append(s).append(",");
            }

            context.write(k2, new Text((sb.delete(sb.length()-1,sb.length()).toString())));
        }
    }

}
