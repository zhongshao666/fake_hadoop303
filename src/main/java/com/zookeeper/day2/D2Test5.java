package com.zookeeper.day2;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class D2Test5 implements Watcher {

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            Event.EventType type = watchedEvent.getType();
            System.out.println("本次是"+type);
            //最后重新绑定事件
            ZooKeeper zk = D2Z.getZk();
            zk.exists(watchedEvent.getPath(), this);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{
        ZooKeeper zk = D2Z.getZk();
        zk.exists("/c1", new D2Test5());
        Thread.sleep(60000);
        zk.close();
    }
}
