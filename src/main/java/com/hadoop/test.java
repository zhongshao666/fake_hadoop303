package com.hadoop;

public class test {
    public static void main(String[] args) {
        String string = "import org.apache.hadoop.io.Text;";
        String s = string.replaceAll("[.;,?:]", " ");
        String[] split = s.split("\\s+");
        for (int i = 0; i <split.length ; i++) {
            System.out.println(split[i]);
        }
    }

}
