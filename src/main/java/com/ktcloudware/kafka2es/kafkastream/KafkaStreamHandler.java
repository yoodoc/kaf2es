/**
 * KafkaStream 인스턴스로 부터 Data을 읽고, KafkaDataJob 객체를 이용해 data 처리한다.  
 * 
 * @author yoodoc@gmail.com
 */

package com.ktcloudware.kafka2es.kafkastream;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class KafkaStreamHandler implements Runnable {
    private KafkaStream<byte[], byte[]> stream;
    private int threadNumber;
    KafkaStreamJob worker;

    public KafkaStreamHandler(KafkaStream<byte[], byte[]> a_stream,
	    int a_threadNumber, KafkaStreamJob job) {
	threadNumber = a_threadNumber;
	stream = a_stream;
	this.worker = job;
    }

    @Override
    public void run() {
	try{
        	ConsumerIterator<byte[], byte[]> it = stream.iterator();
        	while (it.hasNext()) {
        	    System.out.println("excute at " + threadNumber);
        	    this.worker.excute(new String(it.next().message()));
        	}
	}catch (Exception e) {
	    e.printStackTrace();
	} finally {
		this.worker.shutdown();
	}
    }
}