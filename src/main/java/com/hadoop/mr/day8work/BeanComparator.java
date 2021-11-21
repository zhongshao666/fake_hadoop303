/*
package com.hadoop.mr.day8work;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class BeanComparator extends WritableComparator {
    public BeanComparator(){
        super(Bean.class, true);
    }

    //分组比较器
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Bean a1 = (Bean) a;
        Bean b1 = (Bean) b;
        //调用Bean中compareTo
        return a1.getCount().compareTo(b1.getCount());
    }
}
*/
