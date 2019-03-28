package com.stone.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

//根据指定的Topic，Partition，Offset获取数据
public class LowerConsumer {
	
	public static void main(String[] args) {
		
		//定义相关参数
		//kafka集群
		ArrayList<String> brokers = new ArrayList<>();
		brokers.add("172.30.60.62");
		brokers.add("172.30.60.63");
		brokers.add("172.30.60.64");
		
		//端口号
		int port = 9092;
		
		//topic
		String topic = "first";
		
		//partition
		int partition = 1;
		
		//offset
		long offset = 140;
		
		LowerConsumer lowerConsumer = new LowerConsumer();
		lowerConsumer.getData(brokers, port, topic, partition, offset);
		
	}
	
	//找分区leader
	private BrokerEndPoint findLeader(List<String> brokers, int port, String topic, int partition) {
		for (String broker : brokers) {
			//创建获取分区leader的消费者对象
			SimpleConsumer getLeader = new SimpleConsumer(broker, port, 1000, 1024*4, "getLeader");
			//创建一个主题元数据信息请求
			TopicMetadataRequest request = new TopicMetadataRequest(Collections.singletonList(topic));
			//获取主题元数据返回值
			TopicMetadataResponse metadataResponse = getLeader.send(request);
			//解析元数据返回值
			List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();
			//遍历主题元数据
			for (TopicMetadata topicMetadata : topicsMetadata) {
				//获取多个分区的元数据信息
				List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
				//遍历分区元数据
				for (PartitionMetadata partitionMetadata : partitionsMetadata) {
					if (partition == partitionMetadata.partitionId()) {
						return partitionMetadata.leader();
					}
				}
			}
		}
		return null;
	}
	
	//获取数据
	private void getData(List<String> brokers, int port, String topic, int partition, Long offset) {
		//获取分区leader
		BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
		if (leader == null) {
			return;
		}
		
		String host = leader.host();
		//获取数据的消费者对象
		SimpleConsumer getData = new SimpleConsumer(host, port, 1000, 1024*4, "getData");
		//创建获取数据的对象
		FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 102400*4).build();
		//获取数据返回值
		FetchResponse fetchResponse = getData.fetch(fetchRequest);
		//解析返回值
		ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
		//遍历并打印
		for (MessageAndOffset messageAndOffset : messageAndOffsets) {
			long offset1 = messageAndOffset.offset();
			ByteBuffer payload = messageAndOffset.message().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			System.out.println(offset1 + "-" + new String(bytes));
		}
	}

}
