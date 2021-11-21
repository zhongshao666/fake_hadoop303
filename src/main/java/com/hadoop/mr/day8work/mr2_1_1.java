package com.hadoop.mr.day8work;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class mr2_1_1 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new mr2_1_1(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/data/mr2/data1/*");//输入路径
        Path out = new Path("./day8work1_2");//输出路径

        Job job = Job.getInstance(conf, "day8work1");
        job.setJarByClass(this.getClass());

        job.setMapperClass(mr2Mapper.class);
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

        job.setReducerClass(mr2Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(Text.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class mr2Mapper extends Mapper<LongWritable, Text, Text, Text> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            InputSplit inputSplit = context.getInputSplit();
            FileSplit fileSplit = (FileSplit) inputSplit;
            String name = fileSplit.getPath().getName();

            String[] split = string.split("\\s+");

            for (String str:
                 split) {
                context.write(new Text(str),new Text(name));
            }
        }
    }

    static class mr2Reducer extends Reducer<Text, Text, Text, Text> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<>();

            for (Text text:
                 v2s) {
                String string = text.toString();
                if (map.containsKey(string)) {
                    map.put(string, map.get(string) + 1);
                }
                else {
                    map.put(string, 1);
                }
            }
            StringBuilder sb = new StringBuilder();
            Set<Map.Entry<String, Integer>> entries = map.entrySet();
            for (Map.Entry<String, Integer> map1:entries) {
                sb.append(map1.getKey()).append(":").append(map1.getValue()).append(",");
            }
            sb.delete(sb.length() - 1, sb.length());
            context.write(k2, new Text(sb.toString()));
        }
    }

}
