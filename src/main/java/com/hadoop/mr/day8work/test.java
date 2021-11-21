package com.hadoop.mr.day8work;

import java.util.ArrayList;
import java.util.Collections;

public class test {
    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        list.add("class3");
        list.add("class2");
        list.add("class5");
        list.add("class1");
//        System.out.println(compare("class5","class1"));
        System.out.println(list);
//        Collections.swap(list, 0,3);
        sort(list, 0, list.size() - 1);
        System.out.println(list);
        for (String s :
                list) {
            sb.append(s).append(",");
        }
        System.out.println(sb.toString());
    }

    private static void sort(ArrayList<String> list, int low, int high) {
        System.out.println("low = " + low);
        System.out.println("high = " + high);
        if (low >= high)
            return;
        String key = list.get(low);
        int i = low;
        int j = high;

        while (i < j) {
            //从右往左找比key小或等于
            while ((i < j) && compare(list.get(j), key)) {
                j--;
            }
            //从左往右找比key大或等于

            while ((i < j) && !compare(list.get(i), key)) {
                i++;
            }
            //交换arr ij直到i>=j



            if ((i < j) &&list.get(i).equals(list.get(j))) {
                i++;
            } else {
                Collections.swap(list, i, j);
            }
        }
        System.out.println(list);

        sort(list, low, i - 1);
        sort(list, j + 1, high);
    }

    private static boolean compare(String str1, String str2) {
        int i = str1.compareTo(str2);
        if (i >= 0) {
            //str1>str2
            return true;//true
        }
        return false;
    }
}
