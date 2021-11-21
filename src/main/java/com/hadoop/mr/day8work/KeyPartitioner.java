/*
package com.hadoop.mr.day8work;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<Bean, NullWritable> {

    @Override
    public int getPartition(Bean bean, NullWritable nullWritable, int numPartitions) {


        return bean.getWord().hashCode() * Integer.MAX_VALUE % numPartitions;
    }
}
*/
