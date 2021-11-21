package com.hadoop.mr.day8work;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;


public class mr2_4_2 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new mr2_4_2(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("day8work4_1");//输入路径
        Path out = new Path("day8work4_2");//输出路径

//        conf.set(RegexMapper.PATTERN, "[a-z]");
//        conf.set("mapreduce.mapper.regex", "[a-z]");
//        job.setMapperClass(RegexMapper.class);

        Job job = Job.getInstance(conf, "word2");
        job.setJarByClass(this.getClass());



        job.setMapperClass(mr2_4_2Mapper.class);
        job.setMapOutputKeyClass(Bean.class);//map key2输出格式
        job.setMapOutputValueClass(NullWritable.class);//map value2输出格式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);

        //指定分区
        //job.setPartitionerClass(KeyPartitioner.class);
        //给Job设置以上自定义GroupingComparator
        //job.setGroupingComparatorClass(BeanComparator.class);
        //reduce个数
        //job.setNumReduceTasks();
        //job.setCombinerClass(Class <? extends Reducer>);

        job.setReducerClass(mr2_4_2Reducer.class);
        job.setOutputKeyClass(Text.class);//reduce key3输出格式
        job.setOutputValueClass(Text.class);//reduce value3输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class mr2_4_2Mapper extends Mapper<LongWritable, Text, Bean, NullWritable> {//map输出格式


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\s+");
            context.write(new Bean(new Text(split[0]), new Text(split[1])), NullWritable.get());
        }
    }


    static class mr2_4_2Reducer extends Reducer<Bean, NullWritable, Text, Text> {//reduce输入输出格式

        @Override
        protected void reduce(Bean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key.getWord(), key.getCount());
        }
    }

}
