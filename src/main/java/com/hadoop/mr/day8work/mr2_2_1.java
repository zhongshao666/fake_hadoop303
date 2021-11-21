package com.hadoop.mr.day8work;

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
import java.util.*;


public class mr2_2_1 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new mr2_2_1(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/data/mr2_1_1/data2/class.txt");//输入路径
        Path out = new Path("day8work2_1");//输出路径

        Job job = Job.getInstance(conf, "class");
        job.setJarByClass(this.getClass());

        job.setMapperClass(mr2_2Mapper.class);
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

        job.setReducerClass(mr2_2Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(Text.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class mr2_2Mapper extends Mapper<LongWritable, Text, Text, Text> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            String[] split = string.split("\\s+");
            context.write(new Text(split[0]), new Text(split[1]));
        }
    }

    static class mr2_2Reducer extends Reducer<Text, Text, Text, Text> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {

            ArrayList<String> list = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            for (Text text :
                    v2s) {
                list.add(text.toString());
            }
//            Collections.reverse(list);
            Collections.sort(list);
//            sort(list, 0, list.size() - 1);
            for (String s :
                    list) {
                sb.append(s).append(",");
            }

            context.write(k2, new Text((sb.delete(sb.length() - 1, sb.length()).toString())));

        }
    }

    private static void sort(ArrayList<String> list, int low, int high) {
        if (low >= high)
            return;
        String key = list.get(low);
        int i = low;
        int j = high;

        while (i < j) {
            //从右往左找比key小
            while ((i < j) && compare(list.get(j), key)) {
                j--;
            }
            //从左往右找比key大

            while ((i < j) && !compare(list.get(i), key)) {
                i++;
            }
            //交换arr ij直到i>=j
            if ((i < j) &&list.get(i).equals(list.get(j))) {
                i++;
            } else {
                Collections.swap(list, i, j);
            }
        }
        sort(list, low, i - 1);
        sort(list, j + 1, high);
    }

    private static boolean compare(String str1, String str2) {
        int i = str1.compareTo(str2);
        //str1>str2
        //true
        return i >= 0;
    }

}
