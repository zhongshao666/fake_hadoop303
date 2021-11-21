package com.hadoop.mr.day9work;


import java.util.ArrayList;
import java.util.function.Predicate;

public class test {
    public static void main(String[] args) {
/*        String string = "a.b";
        string.replaceAll("[.]", " ");
        System.out.println(string);*/
        ArrayList<Integer> list = new ArrayList<>();
        list.add(500);
        list.add(1000);
        list.add(1100);
        list.add(2100);
        list.add(1800);

/*        Predicate<Integer> p=new Predicate<Integer>() {
            @Override
            public boolean test(Integer i) {
                return i >= 1500;
            }
        } ;*/

        Predicate<Integer> p= i -> i >= 1500;
//        filter(list, n -> n >= 1500);
        filter(list, p);
    }

    public static boolean filter(ArrayList<Integer> list,Predicate<Integer> p){
        for (Integer i:
             list) {
            if(p.test(i)){
                System.out.println(i);
            }
        }
        return true;
    }
}
