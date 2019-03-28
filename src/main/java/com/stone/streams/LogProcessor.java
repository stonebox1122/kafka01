package com.stone.streams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {
	
	private ProcessorContext context;

	@Override
	public void init(ProcessorContext context) {
		this.context = context;
	}

	@Override
	public void process(byte[] key, byte[] value) {
		//获取一行数据
		String line = new String(value);
		
		//去除">>>"
		line = line.replaceAll(">>>", "");
		value = line.getBytes();
		context.forward(key, value);
	}

	@Override
	public void punctuate(long timestamp) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
