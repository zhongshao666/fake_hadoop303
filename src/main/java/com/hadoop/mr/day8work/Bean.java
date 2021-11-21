package com.hadoop.mr.day8work;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements WritableComparable<Bean> {

    private Text word;
    private Text count;

    public Bean() {
    }

    public Bean(Text word, Text count) {
        this.word = word;
        this.count = count;
    }

    public Text getWord() {
        return word;
    }

    public void setWord(Text word) {
        this.word = word;
    }

    public Text getCount() {
        return count;
    }

    public void setCount(Text count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "Bean{" +
                "word=" + word +
                ", count=" + count +
                '}';
    }

    @Override
    public int compareTo(Bean o) {
        if (o==null)
            throw new RuntimeException();
        else if(this.count.compareTo(o.count)==0)
            return this.word.compareTo(o.getWord());
        return o.getCount().compareTo(this.getCount());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word.toString());
        out.writeUTF(count.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = new Text(in.readUTF());
        count = new Text(in.readUTF());
    }
}
