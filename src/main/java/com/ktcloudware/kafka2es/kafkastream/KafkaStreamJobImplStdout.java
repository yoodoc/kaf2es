package com.ktcloudware.kafka2es.kafkastream;

import java.util.Properties;

public class KafkaStreamJobImplStdout implements KafkaStreamJob {

    @Override
    public KafkaStreamJobResult excute(String data) {
	System.out.println("recieved data = " + data);
	return null;
    }

    /** 
     * properties
     */
    @Override
    public KafkaStreamJobResult excute(String data, Properties properties) {
	System.out.println("recieved data = " + data);
		
	return null;
    }

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

}
