package com.stone.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TimeInterceptor implements ProducerInterceptor<String, String> {

	@Override
	public void configure(Map<String, ?> configs) {
		
	}

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		return new ProducerRecord<String, String>(record.topic(), record.key(), 
				System.currentTimeMillis() + "," + record.key());
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		
	}

	@Override
	public void close() {
		
	}

}
