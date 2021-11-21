package com.flume.day2;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.util.Properties;

/**
 * 客户端实现故障转移<br>
 * 要启动三个flume服务端进行测试<br>
 * 服务端接受数据是AVRO类型<Br>
 * 端口号分别是33333/44444/55555<Br>
 * 先启动三个服务端，在运行当前代码,在发送的时候，故意Ctrl+C停掉某个服务端。
 */
public class FailoverClient {
	public static void main(String[] args) throws EventDeliveryException, InterruptedException {
		Properties props = new Properties();
		// 设置客户端类型, 为默认的故障转移
		props.put("client.type", "default_failover");
		// h1 h2 h3是一组
		props.put("hosts", "h1 h2 h3");
		String host1 = "127.0.0.1:33333";
		String host2 = "127.0.0.1:44444";
		String host3 = "127.0.0.1:55555";
		// 小组内的机器ip和端口
		props.put("hosts.h1", host1);
		props.put("hosts.h2", host2);
		props.put("hosts.h3", host3);
		
		//通过RPC工厂获得RPCClient对象
		RpcClient client = RpcClientFactory.getInstance(props);
		for (int i = 0; i < 10; i++) {
			//把数据构建为Event
			Event event = EventBuilder.withBody(("helloworld " + i).getBytes());
			//发送
			client.append(event);
			Thread.sleep(2000);
		}
		client.close();
	}
}
