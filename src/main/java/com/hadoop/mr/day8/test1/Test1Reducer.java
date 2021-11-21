package com.hadoop.mr.day8.test1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Test1Reducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        double sum = 0;
        for (DoubleWritable doubleWritable :
                values) {
            sum += doubleWritable.get();
            num++;
        }
        context.write(key,new DoubleWritable(sum/num));
    }
}
