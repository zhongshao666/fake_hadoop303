package com.flume.day2;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

/**
 * flume的客户端，只能给服务端发送AVRO类型的数据<br>
 * 当前flume的客户端 输出类型就是AVRO
 * */
public class AvroClient {
	public static void main(String[] args) throws EventDeliveryException, InterruptedException {
		//获取Client
		//使用工厂获取，参数 要连接的flume 服务端ip和端口号
		//注 ：RPC 远程过程调用
		RpcClient client = RpcClientFactory.getDefaultInstance("127.0.0.1", 8099);
		//把当前客户端想要发送出去的数据，封装为Event
		Event event = EventBuilder.withBody(("hello ").getBytes());
		//把数据发送出去
		client.append(event);

		Thread.sleep(2000);
		client.close();
	}
}
