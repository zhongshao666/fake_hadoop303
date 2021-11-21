package com.zookeeper.day1;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class D1Test1 {
    public static void main(String[] args) throws Exception {
        Watcher watcher= watchedEvent -> {
            //本次是用于连接事件Watcher
            //只关注状态
            Watcher.Event.KeeperState state = watchedEvent.getState();
            //获取同步连接成功状态值
            Watcher.Event.KeeperState syncConnected = Watcher.Event.KeeperState.SyncConnected;
            if (state==syncConnected){
                System.out.println("连接成功");
            }else {
                System.out.println("连接失败");
            }
        };
        ZooKeeper zk = new ZooKeeper("192.168.244.100:2181",10000,watcher);
        System.out.println("成功");
        zk.close();
    }
}
