package com.hadoop.mr.day11work;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements WritableComparable<Bean> {
    private Text name;
    private IntWritable id;
    private FloatWritable score;

    public Bean() {
    }

    public Bean(Text name, IntWritable id, FloatWritable score) {
        this.name = name;
        this.id = id;
        this.score = score;
    }

    @Override
    public String toString() {
        return "Bean{" +
                "name=" + name +
                ", id=" + id +
                ", score=" + score +
                '}';
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public IntWritable getId() {
        return id;
    }

    public void setId(IntWritable id) {
        this.id = id;
    }

    public FloatWritable getScore() {
        return score;
    }

    public void setScore(FloatWritable score) {
        this.score = score;
    }

    @Override
    public int compareTo(Bean o) {
        if (o==null)
            throw new RuntimeException();
        if (o.getScore().compareTo(this.score)==0)
            return this.id.compareTo(o.getId());
        return o.getScore().compareTo(this.score);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name.toString());
        out.writeInt(id.get());
        out.writeFloat(score.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = new Text(in.readUTF());
        id = new IntWritable(in.readInt());
        score = new FloatWritable(in.readFloat());
    }
}
