package com.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class HbaseMr extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new HbaseMr(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        conf.set("hbase.zookeeper.quorum", "fake:2181");
        Job job = Job.getInstance(conf);
        //设置驱动类
        job.setJarByClass(HbaseMr.class);
        job.setJobName("mr");
        //设置Mapper
        TableMapReduceUtil.initTableMapperJob("bd05:emp",
                new Scan(),
                HMapper.class,
                Text.class,
                IntWritable.class,
                job);
        //设置reducer
        TableMapReduceUtil.initTableReducerJob("bd05:count",
                HReducer.class,
                job
        );
        job.waitForCompletion(true);
        return 0;
    }

    static class HMapper extends TableMapper<Text, IntWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
//            Bytes.toString(key.get());
//            value.getValue() 一个单元格
//            value.getMap()  .......
//            value.listCells()
            context.write(new Text("bd05:emp"), new IntWritable(1));
        }
    }

    static class HReducer extends TableReducer<Text, IntWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            //行键
            Put put = new Put(Bytes.toBytes(key.toString()));
            //                             列族                    列                        值
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("count"), Bytes.toBytes(sum + ""));
            context.write(NullWritable.get(), put);
        }
    }

}
