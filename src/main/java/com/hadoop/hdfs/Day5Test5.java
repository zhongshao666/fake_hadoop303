package com.hadoop.hdfs;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.Arrays;

//bean
public class Day5Test5 implements Writable {


    public static void main(String[] args) throws Exception {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        Day5Test5 t = new Day5Test5(1,"tooom",30);
        t.write(new DataOutputStream(b));
        //对象在buf字节数组中
        byte[] buf = b.toByteArray();
        System.out.println(Arrays.toString(buf));

        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        DataInputStream dis = new DataInputStream(bais);
        Day5Test5 tt = new Day5Test5();
        tt.readFields(dis);

        System.out.println(tt);


    }

    private int id;
    private String name;
    private int age;

    public Day5Test5() {
    }

    public Day5Test5(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //序列化id
        IntWritable iwid = new IntWritable(id);
        iwid.write(out);
        //序列化name
        Text tname = new Text(name);
        tname.write(out);
        IntWritable iage = new IntWritable(age);
        iage.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        IntWritable iwid = new IntWritable();
        Text tname = new Text();
        IntWritable iage = new IntWritable();

        iwid.readFields(in);
        tname.readFields(in);
        iage.readFields(in);

        this.id = iwid.get();
        this.name = tname.toString();
        this.age = iage.get();
    }

    @Override
    public String toString() {
        return "Day5Test5{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
