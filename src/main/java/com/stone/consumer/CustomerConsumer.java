package com.stone.consumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class CustomerConsumer {

	public static void main(String[] args) {

		//配置信息
		Properties props = new Properties();
		//kafka集群
		props.put("bootstrap.servers", "172.30.60.62:9092");
		//消费者组id
		props.put("group.id", "test");
		//更换组的情况下，重复消费数据指定属性
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		//设置自动提交offset
		props.put("enable.auto.commit", "true");
		//提交延时
		props.put("auto.commit.interval.ms", "1000");
		//反序列化
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		//创建消费者对象
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		//指定topic
		consumer.subscribe(Arrays.asList("first","second"));
		//不更换组的情况下，指定偏移量，重复消费数据
		//consumer.assign(Collections.singleton(new TopicPartition("first", 0)));
		//consumer.seek(new TopicPartition("first", 0), 2);
		
		while (true) {
			//获取 数据
			ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				System.out.println(consumerRecord.topic() + "--" 
						+ consumerRecord.partition() + "--" 
						+ consumerRecord.value());
			}
		}

	}

}
