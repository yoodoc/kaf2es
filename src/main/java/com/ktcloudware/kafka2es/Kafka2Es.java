/**
 * @author yoodoc@gmail.com
 */

package com.ktcloudware.kafka2es;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.ParseException;

import com.ktcloudware.kafka2es.kafkaclient.KafkaGroupedConsumer;
import com.ktcloudware.kafka2es.kafkastream.KafkaStreamJobImplES;
import com.ktcloudware.kafka2es.kafkastream.KafkaStreamJobImplStdout;
import com.ktcloudware.kafka2es.kafkastream.KafkaStreamHandler;
import com.ktcloudware.kafka2es.kafkastream.KafkaStreamJob;
import com.ktcloudware.kafka2es.util.OptionParser;

import kafka.consumer.KafkaStream;

public class Kafka2Es {

	ExecutorService executor;
	public static final String OPTION_ZOOKEEPER = "zookeeper";
	public static final String OPTION_GROUP = "group";
	public static final String OPTION_TOPIC = "topic";
	private static final String OPTION_FETCH_SIZE = "fetchsize";
	public static final String OPTION_ENABLE_ES_INSERT = "enable";
	public static final String OPTION_ES_ADDRESS = "elasticsearch";
	public static final String OPTION_ES_CLUSTER_NAME = "clustername";
	public static final String OPTION_ES_INDEX_NAME = "indexname";
	public static final String OPTION_ES_TYPE_NAME = "typename";
	public static final String OPTION_ES_ROUTINGKEY = "routingkey";
	public static final String OPTION_ES_BULKSIZE = "bulksize";
	public static final String OPTION_ES_BULK_INTERVAL_SEC = "bulkinterval";

	/*
	 * default broker zookeeper url 14.63.199.135
	 */
	public static void main(String[] args) throws ParseException {
		//read options 
		Map<String, String> argMap = OptionParser.parseCmdOptions(args);
		if (argMap == null || argMap.isEmpty()) return;
		String zooKeeper = argMap.get(OPTION_ZOOKEEPER);
		String groupId = argMap.get(OPTION_GROUP);
		String topic = argMap.get(OPTION_TOPIC);
		boolean enableES = Boolean.parseBoolean(argMap
				.get(OPTION_ENABLE_ES_INSERT));
		String esAddress = argMap.get(OPTION_ES_ADDRESS);
		String clusterName = argMap.get(OPTION_ES_CLUSTER_NAME);
		String indexName = argMap.get(OPTION_ES_INDEX_NAME);
		String typeName = argMap.get(OPTION_ES_TYPE_NAME);
		String routingKey = argMap.get(OPTION_ES_ROUTINGKEY);
		int esBulkSize = Integer.valueOf(argMap.get(OPTION_ES_BULKSIZE));
		int esBulkMaxInterval = Integer.valueOf(argMap
				.get(OPTION_ES_BULK_INTERVAL_SEC));
		KafkaStreamJob job = null;

		//check requeired options
		
		//create job instance for data handling
		if (enableES) {
			try {
				job = new KafkaStreamJobImplES(esAddress, clusterName,
						indexName, typeName, routingKey, esBulkSize,
						esBulkMaxInterval);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			job = new KafkaStreamJobImplStdout();
		}

		//create kafka consumer group & make consumer stream 
		KafkaGroupedConsumer consumerGroup = new KafkaGroupedConsumer(
				zooKeeper, groupId, 1);
		List<KafkaStream<byte[], byte[]>> streams = consumerGroup
				.getMessageStreams(topic);
		ExecutorService executor = Executors.newFixedThreadPool(1);

		try {
			executor.submit(new KafkaStreamHandler(streams.get(0), 0, job))
					.get();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			executor.shutdown();
			consumerGroup.close();
		}
	}
}