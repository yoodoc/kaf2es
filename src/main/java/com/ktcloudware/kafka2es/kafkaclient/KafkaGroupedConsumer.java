/**
 * 
 * @author yoodoc@gmail.com
 */
package com.ktcloudware.kafka2es.kafkaclient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaGroupedConsumer {
	ConsumerConnector kakfaConnector;
	String consumerGroupId;
	String zkAddress;
	int m_numOfThread = 1;
	
	public KafkaGroupedConsumer(String zkAddress, String consumerGroupId, int numOfThread) {
		this.consumerGroupId = consumerGroupId;
		this.zkAddress = zkAddress;
		this.m_numOfThread = numOfThread > 0 ? numOfThread : 1;
		connect(zkAddress, consumerGroupId);
	}
	
	void connect(String zk, String groupId) {
		ConsumerConfig consumerConfig = createConsumerConfig(zk, groupId);
		try {
			this.kakfaConnector = kafka.consumer.Consumer
					.createJavaConsumerConnector(consumerConfig);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public List<KafkaStream<byte[], byte[]>> getMessageStreams(String topic) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, m_numOfThread);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = kakfaConnector
				.createMessageStreams(topicCountMap);
		if (consumerMap == null)
			return null;
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		return streams;
	}

	public void close() {
		kakfaConnector.shutdown();
		kakfaConnector = null;
	}

	private static ConsumerConfig createConsumerConfig(String a_zookeeper, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("fetch.size", "500000");

		return new ConsumerConfig(props);
	}

}
