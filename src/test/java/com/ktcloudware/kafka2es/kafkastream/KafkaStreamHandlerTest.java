package com.ktcloudware.kafka2es.kafkastream;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class KafkaStreamHandlerTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testConstruct() {
	/* KafkaStream<byte[], byte[]> mockStream = createMock(KafkaStream.class);
	KafkaStreamJob mockStreamJob = createMock(KafkaStreamJobImplStdout.class);
	int a_threadNumber = 1;
	KafkaStreamHandler streamHandler = new KafkaStreamHandler(mockStream, a_threadNumber, mockStreamJob);
	
	assertEquals(streamHandler.worker, mockStreamJob);*/
    }
    
    @Test
    public void testRun() {
	/*//create mock 
	KafkaStream<byte[], byte[]> mockStream = createMock(KafkaStream.class);
	KafkaStreamJob mockStreamJob = createMock(KafkaStreamJobImplStdout.class);
	ConsumerIterator<?, ?> mockConsumerIterator = createMock(ConsumerIterator.class);
	MessageAndMetadata<byte[], byte[]> mockMessageAndMetadata = createMock(MessageAndMetadata.class);
	int a_threadNumber = 1;
	byte[] data = "data".getBytes();
	KafkaStreamHandler streamHandler = new KafkaStreamHandler(mockStream, a_threadNumber, mockStreamJob);
	
	//create expect
	expect(mockStreamJob.excute(anyObject(String.class))).andReturn(new KafkaStreamJobResult());
	expect(mockStream.iterator()).andReturn((ConsumerIterator<byte[], byte[]>) mockConsumerIterator);
	expect(mockConsumerIterator.hasNext()).andReturn(true);
	expect(mockConsumerIterator.next()).andReturn((MessageAndMetadata<?, ?>) mockMessageAndMetadata);
	expect(mockMessageAndMetadata.message()).andReturn(data);
	expect(mockConsumerIterator.hasNext()).andReturn(false);

	//create replay
	replay(mockStreamJob);
	replay(mockStream);
	replay(mockConsumerIterator);
	replay(mockMessageAndMetadata);
	
	streamHandler.run();
	//Verifying Mock Behavior
	verify(mockStreamJob);*/
    }
}
