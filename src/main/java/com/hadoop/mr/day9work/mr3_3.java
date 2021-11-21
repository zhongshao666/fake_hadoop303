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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class mr3_3 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new mr3_3(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {


        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "jobName");
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(2);

        job.setMapperClass(mr3_3Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);//map key2输出格式
        job.setMapOutputValueClass(Text.class);//map value2输出格式

        job.setReducerClass(mr3_3Reducer1.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("/data/mr3/grade.txt"));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path("mr3_3_1"));

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class mr3_3Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\s+");
            context.write(new IntWritable(Integer.parseInt(split[1])), new Text(split[0]));
        }
    }

    static class mr3_3Reducer1 extends Reducer<IntWritable, Text, IntWritable, Text> {//reduce输入输出格式

        @Override
        protected void reduce(IntWritable k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text t :
                    v2s) {
                sb.append(t.toString()).append(",");
            }
            sb.delete(sb.length() - 1, sb.length());
            context.write(k2, new Text(sb.toString()));
        }
    }

/*    static class mr3_3Reducer2 extends Mapper<Bean, Text, Text, Text> {//reduce输入输出格式

        @Override
        protected void map(Bean key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);
            context.write(key.getScore(), value);
        }
    }*/

}
