package com.zookeeper.day2;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class D5test6 implements Watcher {

    @Override
    public void process(WatchedEvent watchedEvent) {

    }

    public static void main(String[] args) {
        ZooKeeper zk = D2Z.getZk();

    }
}
