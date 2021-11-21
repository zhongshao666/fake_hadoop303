package com.zookeeper.day2;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class Day2Test1 {
    public static void main(String[] args) throws Exception {
        //创建信号量
        CountDownLatch c = new CountDownLatch(3);
        //信号量减少
        c.countDown();
        c.countDown();
        c.countDown();
        System.out.println("0000");
        c.await();
        System.out.println(".....");

        CountDownLatch cdl = new CountDownLatch(1);
        Watcher watcher=watchedEvent -> {
            Watcher.Event.KeeperState state = watchedEvent.getState();
            Watcher.Event.KeeperState sc = Watcher.Event.KeeperState.SyncConnected;
            if (sc == state) {
                System.out.println("连接成功");
                cdl.countDown();
            }else {
                System.out.println("连接失败");
            }
        } ;

        ZooKeeper zk = new ZooKeeper("fake:2181", 1000,watcher );
        cdl.await();
        System.out.println(zk);
    }
}
