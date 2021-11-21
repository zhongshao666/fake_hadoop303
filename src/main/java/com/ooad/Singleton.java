package com.ooad;

import java.util.HashMap;
import java.util.Map;

public class Singleton {
    private static Singleton instance = new Singleton();
    private Singleton(){}
    public static Singleton getInstance(){
        return instance;
    }
    public void show(){
        System.out.println("666");
    }
    public static void main(String[] args){
        Map<Object, Object> hashMap = new HashMap<>();
        Singleton instance = getInstance();
        instance.show();
    }
}

class lan1 {
    private static lan1 l1;
    private lan1(){}
    public static lan1 getInstance(){
        if(l1==null){
            l1 = new lan1();
        }
        return l1;
    }

}