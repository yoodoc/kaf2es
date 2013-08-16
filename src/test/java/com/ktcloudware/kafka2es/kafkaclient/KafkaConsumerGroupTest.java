package com.ktcloudware.kafka2es.kafkaclient;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ktcloudware.kafka2es.kafkaclient.KafkaGroupedConsumer;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class KafkaConsumerGroupTest {
	KafkaGroupedConsumer consumerGroup;
	ConsumerConnector mockConnector;
	@Before
	public void setUp() throws Exception {
		mockConnector = createMock(ConsumerConnector.class);

		final ConsumerConnector conn = mockConnector;
		consumerGroup = new KafkaGroupedConsumer("zookeeper", "groupId", 1){
			void connect(String zk, String groupId) 
			{
				//this.curator = cur;
				this.kakfaConnector = conn;
			};
		};
	}

	@After
	public void tearDown() throws Exception {
		mockConnector = null;
		consumerGroup = null;
	}

	@Test
	public void testConstructor()
	{
		replay(mockConnector);
		assertEquals(consumerGroup.consumerGroupId, "groupId");
	}

	@Test
	public void testClose()
	{
		mockConnector.shutdown();
		expectLastCall().asStub();
		replay(mockConnector);
		
		consumerGroup.close();
		verify(mockConnector);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetMessageStreams()
	{
		Map<String, List<KafkaStream<byte[], byte[]>>> resultStreams = new HashMap<String, List<KafkaStream<byte[],byte[]>>>();
		List<KafkaStream<byte[],byte[]>> streamList = new ArrayList<KafkaStream<byte[],byte[]>>();
		KafkaStream<byte[], byte[]> stream = new KafkaStream<byte[], byte[]>(null, 0, null, null, "client-id");
		streamList.add(stream);
		resultStreams.put("unittest-topic", streamList);
		expect(mockConnector.createMessageStreams(anyObject(HashMap.class))).andReturn(resultStreams);
		replay(mockConnector);	
		consumerGroup.getMessageStreams("my_topic");
		verify(mockConnector);
	}
}
