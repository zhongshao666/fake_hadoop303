package com.hadoop.mr.day8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.StringTokenizer;


//倒置索引
public class Test2 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Test2(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        //读取四个文件
        //方案一  ： 放同一在文件夹，读取文件夹
        //方案二  ： TextInputFormat.addInputPaths(job,in1);
        //方案三  ： MultipleInput...

        Path in = new Path("/data/4_倒置索引表");//输入路径
        Path out = new Path("./day8test2");//输出路径

        Job job = Job.getInstance(conf, "索引");
        job.setJarByClass(this.getClass());


        job.setMapperClass(Test2Mapper.class);
        job.setMapOutputKeyClass(Text.class);//map key2输出格式
        job.setMapOutputValueClass(Text.class);//map value2输出格式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);
//        TextInputFormat.addInputPaths(job,in1);

        //指定分区
        //job.setPartitionerClass(?.class);
        //给Job设置以上自定义GroupingComparator
        //job.setGroupingComparatorClass(?.class);
        //reduce个数
        //job.setNumReduceTasks();

        job.setReducerClass(Test2Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(Text.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class Test2Mapper extends Mapper<LongWritable, Text, Text, Text> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            //获取文件名 输入分片
            InputSplit inputSplit = context.getInputSplit();
            //输入分片强转问文件分片
            FileSplit fileSplit = (FileSplit) inputSplit;
            String name = fileSplit.getPath().getName();

            //去除空格 \t..
            StringTokenizer stringTokenizer = new StringTokenizer(string);
            while (stringTokenizer.hasMoreTokens()){
                context.write(new Text(stringTokenizer.nextToken()), new Text(name));
            }

        }
    }

    static class Test2Reducer extends Reducer<Text, Text, Text, Text> {//reduce输入输出格式

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> hashMap = new HashMap<>();

            for (Text t : v2s) {
                String string = t.toString();
                if (!hashMap.containsKey(string)){
                    hashMap.put(string, 1);
                }else {
                    hashMap.put(string, hashMap.get(string) + 1);
                }
            }
            StringBuilder sb=new StringBuilder();
            Set<Map.Entry<String, Integer>> entries = hashMap.entrySet();
            for (Map.Entry<String, Integer> map:
                 entries) {
                sb.append(map.getKey()).append(":").append(map.getValue()).append(",");
            }
            context.write(k2, new Text(sb.toString().substring(0,sb.length()-1)));

        }
    }

}
