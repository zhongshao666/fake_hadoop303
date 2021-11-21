package com.flume.day2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.event.JSONEvent;

import com.google.gson.Gson;

/**
 * 发送json给T7.conf<br>
 * 
 */
public class Test {
	public static void main(String[] args) throws EventDeliveryException {
		// flume拿数据的时候都会封装为一个Event，这里是一个JsonEvent。
		JSONEvent je = new JSONEvent();

		// Event，里面两个部分，一个头，一个体。都可以放数据。

		// 向event体中放数据
		je.setBody("helloworld".getBytes());
		HashMap<String, String> map = new HashMap<>();
		map.put("bttc", "2");
		// 向event头中放数据
		je.setHeaders(map);

		List list = new ArrayList();
		list.add(je);
		// 官网要求把jsonevent通过Gson转换为json字符串。
		Gson gson = new Gson();
		String s = gson.toJson(list);

		// 发送 POST 请求
		String sr = HttpRequest.sendPost("http://127.0.0.1:8888", s);
		System.out.println(sr);

		// RpcClient client = RpcClientFactory.
		// getDefaultInstance("192.168.117.50", 44444);
		// Event event = EventBuilder.withBody
		// ("helloworld".getBytes());
		//
		//
		// client.append(je);
		// client.close();

	}
}
