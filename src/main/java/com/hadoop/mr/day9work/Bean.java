/*
package com.hadoop.mr.day9work;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Bean implements WritableComparable<Bean> {
    private Text score;

    public Bean() {
    }

    public Bean(Text score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "Bean{" +
                "score=" + score +
                '}';
    }

    public Text getScore() {
        return score;
    }

    public void setScore(Text score) {
        this.score = score;
    }

    @Override
    public int compareTo(Bean o) {
        if (o == null) {
            throw new RuntimeException();
        }
//        return o.getScore().compareTo(this.score);
        if(this.score.compareTo(o.score)<0)//String类型的大小比较
        {
            return 1;
        }
        else if(this.score.compareTo(o.score)>0)
        {
            return -1;
        }
        return 0;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(score.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        score = new Text(in.readUTF());
    }
}
*/
