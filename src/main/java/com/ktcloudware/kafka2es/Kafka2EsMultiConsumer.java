/**
 * @author yoodoc@gmail.com
 */

package com.ktcloudware.kafka2es;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.ktcloudware.kafka2es.kafkaclient.KafkaGroupedConsumer;
import com.ktcloudware.kafka2es.kafkastream.KafkaStreamJobImplStdout;
import com.ktcloudware.kafka2es.kafkastream.KafkaStreamHandler;
import com.ktcloudware.kafka2es.kafkastream.KafkaStreamJob;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class Kafka2EsMultiConsumer {
	private ConsumerConnector consumer;
	private final String kafkaTopicName;
	private final String consumerGroupId;
	private final String zkAddress;
	private KafkaGroupedConsumer consumerGroup;
	ExecutorService executor;

	private static final String OPTION_ZOOKEEPER = "zookeeper";
	private static final String OPTION_GROUP = "group";
	private static final String OPTION_TOPIC = "topic";
	private static final String OPTION_FETCH_SIZE = "fetchsize";

	public Kafka2EsMultiConsumer(String zkAddress, String consumerGroupId,
			String kakfaTopicName) {
		this.consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(zkAddress,
						consumerGroupId));
		this.kafkaTopicName = kakfaTopicName;
		this.consumerGroupId = consumerGroupId;
		this.zkAddress = zkAddress;
	}

	public void shutdown() {
		if (executor != null)
			executor.shutdown();
		if (consumer != null)
			consumer.shutdown();
		if (consumerGroup != null)
			consumerGroup.close();
	}

	public void run(int numOfThreads) {
		consumerGroup = new KafkaGroupedConsumer(zkAddress, consumerGroupId,
				numOfThreads);
		List<KafkaStream<byte[], byte[]>> streams = consumerGroup
				.getMessageStreams(kafkaTopicName);
		// now launch all the threads
		executor = Executors.newFixedThreadPool(numOfThreads);

		// now create an object to consume the messages
		int threadNumber = 0;
		KafkaStreamJob job = new KafkaStreamJobImplStdout();
		try {
			for (KafkaStream<byte[], byte[]> stream : streams) {

				executor.submit(new KafkaStreamHandler(stream, threadNumber,
						job));
				threadNumber++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static ConsumerConfig createConsumerConfig(String zkAddress,
			String consumerGroupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zkAddress);
		props.put("group.id", consumerGroupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);
	}

	public static void main(String[] args) throws ParseException {
		Map<String, String> argMap = parseCmdOptions(args);
		if (argMap == null || argMap.isEmpty())
			return;
		/*
		 * String zooKeeper = "14.63.199.135"; String groupId =
		 * "kafka2esLocal2"; String topic = "yoodoc1";
		 */
		String zooKeeper = argMap.get(OPTION_ZOOKEEPER);
		String groupId = argMap.get(OPTION_GROUP);
		String topic = argMap.get(OPTION_TOPIC);
		String fetchSize = argMap.get(OPTION_TOPIC);

		Kafka2EsMultiConsumer worker = new Kafka2EsMultiConsumer(zooKeeper,
				groupId, topic);
		int numOfthreads = 1;

		worker.run(numOfthreads);
		try {
			Thread.sleep(1000000);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		worker.shutdown();
	}

	private static Map<String, String> parseCmdOptions(String[] args)
			throws ParseException {
		Options options = BuildOptions();
		CommandLineParser parser = new GnuParser();
		CommandLine cmd = parser.parse(options, args);

		if (!cmd.hasOption(OPTION_ZOOKEEPER) || !cmd.hasOption(OPTION_GROUP)
				|| !cmd.hasOption(OPTION_TOPIC)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("kafka2es [OPTION]...", options);
			return null;
		}
		Map<String, String> map = new HashMap<String, String>();
		String zkAddress = cmd.getOptionValue(OPTION_ZOOKEEPER);
		String groupId = cmd.getOptionValue(OPTION_GROUP);
		String topic = cmd.getOptionValue(OPTION_TOPIC);

		// not implemented
		String fetchSize = cmd.getOptionValue(OPTION_GROUP);
		String bulkInsertSize;

		map.put(OPTION_ZOOKEEPER, zkAddress);
		map.put(OPTION_GROUP, groupId);
		map.put(OPTION_TOPIC, topic);

		return map;
	}

	private static Options BuildOptions() {
		@SuppressWarnings("static-access")
		Option listener = OptionBuilder
				.withArgName("urls")
				.hasArg()
				.withDescription(
						"REQUIRED:  The connection string for the zookeeper connection in the form host:port. Multiple URLS can be given to allow fail-over.")
				.create(OPTION_ZOOKEEPER);
		@SuppressWarnings("static-access")
		Option group = OptionBuilder.withArgName("group name").hasArg()
				.withDescription("REQUIRED: The group id to consume on.")
				.create(OPTION_GROUP);
		@SuppressWarnings("static-access")
		Option topic = OptionBuilder.withArgName("topic name").hasArg()
				.withDescription("REQUIRED: The topic id to consume on.")
				.create(OPTION_TOPIC);
		@SuppressWarnings("static-access")
		Option fetchSize = OptionBuilder
				.withArgName("urls")
				.hasArg()
				.withDescription(
						"The amount of data to fetch in a single request. (default: 1048576)")
				.create(OPTION_FETCH_SIZE);

		Options options = new Options();
		options.addOption(listener);
		options.addOption(fetchSize);
		options.addOption(group);
		options.addOption(topic);
		return options;
	}
}