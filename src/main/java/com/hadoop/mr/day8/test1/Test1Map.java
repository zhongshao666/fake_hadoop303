package com.hadoop.mr.day8.test1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//天气
public class Test1Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    /**
     * @Description TODO
     * 原始数据
     **/
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String sid = string.substring(0, 15);
        String year = string.substring(15, 19);
        String temp = string.substring(87, 92);
        String jug = string.substring(92, 93);
        if (!"+9999".equals(temp)) {
            if (jug.matches("[01459]")){
                context.write(new Text(sid+"_"+year),new DoubleWritable(Double.parseDouble(temp)));
            }
        }
    }
}
