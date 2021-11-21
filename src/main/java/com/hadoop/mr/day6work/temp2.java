package com.hadoop.mr.day6work;

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


public class temp2 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new temp2(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/weather");//输入路径
        Path out = new Path("/out_wea2");//输出路径

        Job job = Job.getInstance(conf, "wea2");
        job.setJarByClass(this.getClass());

        job.setMapperClass(temp2Mapper.class);
        job.setMapOutputKeyClass(Text.class);//map key2输出格式
        job.setMapOutputValueClass(IntWritable.class);//map value2输出格式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);

        //指定分区
        //job.setPartitionerClass(?.class);
        //给Job设置以上自定义GroupingComparator
        //job.setGroupingComparatorClass(?.class);

        job.setReducerClass(temp2Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(FloatWritable.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class temp2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            String substring1 = string.substring(19, 21);
            int substring2 = Integer.parseInt(string.substring(87, 91));
            String substring3 = string.substring(91, 92);
            if (!(substring2 + substring3).equals("+9999"))
                if (substring3.equals("0") || substring3.equals("1") || substring3.equals("4") || substring3.equals("5") || substring3.equals("9")) {
                    switch (substring1) {
                        case "01":
                        case "02":
                        case "03":
                            context.write(new Text("第一季度"), new IntWritable(substring2));
                            break;
                        case "04":
                        case "05":
                        case "06":
                            context.write(new Text("第二季度"), new IntWritable(substring2));
                            break;
                        case "07":
                        case "08":
                        case "09":
                            context.write(new Text("第三季度"), new IntWritable(substring2));
                            break;
                        case "10":
                        case "11":
                        case "12":
                            context.write(new Text("第四季度"), new IntWritable(substring2));
                            break;
                    }
                /*
                                if (substring1.equals("01")||substring1.equals("02")||substring1.equals("03"))
                    context.write(new Text("第一季度"), new IntWritable(substring2));
                else if(substring1.equals("04")||substring1.equals("05")||substring1.equals("06"))
                    context.write(new Text("第二季度"), new IntWritable(substring2));
                else if(substring1.equals("07")||substring1.equals("08")||substring1.equals("09"))
                    context.write(new Text("第三季度"), new IntWritable(substring2));
                else if(substring1.equals("10")||substring1.equals("11")||substring1.equals("12"))
                    context.write(new Text("第四季度"), new IntWritable(substring2));
                 */
                }

        }
    }

    static class temp2Reducer extends Reducer<Text, IntWritable, Text, FloatWritable> {//reduce输入输出格式


        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (IntWritable i :
                    v2s) {
                sum += (float) i.get();
                count++;
            }
            context.write(k2, new FloatWritable(sum / count));
        }
    }

}
