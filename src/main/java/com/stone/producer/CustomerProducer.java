package com.stone.producer;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CustomerProducer {

	public static void main(String[] args) {

		//配置信息
		Properties props = new Properties();
		//Kafka集群
		props.put("bootstrap.servers", "172.30.60.62:9092");
		//应答级别
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.stone.producer.CustomerPartitioner");
		//拦截器，按照添加顺序执行
		ArrayList<String> list = new ArrayList<String>();
		list.add("com.stone.interceptor.TimeInterceptor");
		list.add("com.stone.interceptor.CountInterceptor");
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 10; i++) {
			producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)), new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						System.out.println(metadata.partition() + "--" + metadata.offset());
					} else {
						System.out.println("发送失败");
					}
				}
			});
		}
		producer.close();
	}

}
