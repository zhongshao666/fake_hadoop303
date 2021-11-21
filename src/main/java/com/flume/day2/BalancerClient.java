package com.flume.day2;

import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

/**
 * 客户端实现负载均衡
 * 要启动三个flume服务端进行测试<br>
 * 服务端接受数据是AVRO类型<Br>
 * 端口号分别是33333/44444/55555<Br>
 * 先启动三个服务端，在运行当前代码
 */
public class BalancerClient {
	public static void main(String[] args) throws EventDeliveryException, InterruptedException {
		Properties props = new Properties();
		// 设置客户端类型, 为默认的负载均衡
		props.put("client.type", "default_loadbalance");

		props.put("hosts", "h1 h2 h3");

		// Create the client with load balancing properties
		String host1 = "127.0.0.1:33333";
		String host2 = "127.0.0.1:44444";
		String host3 = "127.0.0.1:55555";
		props.put("hosts.h1", host1);
		props.put("hosts.h2", host2);
		props.put("hosts.h3", host3);

		props.put("host-selector", "random"); // For random host selection
		props.put("backoff", "true"); // Disabled by default.
		props.put("maxBackoff", "10000"); // Defaults 0, which effectively

		RpcClient client = RpcClientFactory.getInstance(props);

		for (int i = 0; i < 20; i++) {
			Event event = EventBuilder.withBody(("helloworld" + i).getBytes());
			client.append(event);
			Thread.sleep(2000);
		}
		client.close();
	}
}
