/**
 * @author yoodoc@gmail.com
 */

package com.ktcloudware.kafka2es;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.ktcloudware.kafka2es.kafkaclient.KafkaGroupedConsumer;
import com.ktcloudware.kafka2es.kafkastream.KafkaStreamJobImplES;
import com.ktcloudware.kafka2es.kafkastream.KafkaStreamJobImplStdout;
import com.ktcloudware.kafka2es.kafkastream.KafkaStreamHandler;
import com.ktcloudware.kafka2es.kafkastream.KafkaStreamJob;

import kafka.consumer.KafkaStream;

public class Kafka2Es {
   
    ExecutorService executor;
    private static final String OPTION_ZOOKEEPER = "zookeeper";
    private static final String OPTION_GROUP = "group";
    private static final String OPTION_TOPIC = "topic";
    private static final String OPTION_FETCH_SIZE = "fetchsize";
    private static final String OPTION_ENABLE_ES_INSERT = "enable";
    private static final String OPTION_ES_ADDRESS = "elasticsearch";
    private static final String OPTION_ES_CLUSTER_NAME = "clustername";
    private static final String OPTION_ES_INDEX_NAME = "indexname";
	private static final String OPTION_ES_TYPE_NAME = "typename";
	private static final String OPTION_ES_ROUTINGKEY= "routingkey";
	private static final String OPTION_ES_BULKSIZE = "bulksize";
	private static final String OPTION_ES_BULK_INTERVAL_SEC = "bulkinterval";
	
    /*
     * default broker zookeeper url 14.63.199.135
     */
    public static void main(String[] args) throws ParseException {
	Map<String, String> argMap = parseCmdOptions(args);
	if (argMap == null || argMap.isEmpty())
	    return;
	String zooKeeper = argMap.get(OPTION_ZOOKEEPER);
	String groupId = argMap.get(OPTION_GROUP);
	String topic = argMap.get(OPTION_TOPIC);
	boolean enableES = Boolean.parseBoolean(argMap.get(OPTION_ENABLE_ES_INSERT));
	String esAddress = argMap.get(OPTION_ES_ADDRESS);
	String clusterName = argMap.get(OPTION_ES_CLUSTER_NAME);
	String indexName = argMap.get(OPTION_ES_INDEX_NAME);
	String typeName = argMap.get(OPTION_ES_TYPE_NAME);
	String routingKey = argMap.get(OPTION_ES_ROUTINGKEY);
	int esBulkSize = Integer.valueOf(argMap.get(OPTION_ES_BULKSIZE));
	int esBulkMaxInterval = Integer.valueOf(argMap.get(OPTION_ES_BULK_INTERVAL_SEC));
	KafkaStreamJob job = null ;
	
	if(enableES) {
	    try {
		job = new KafkaStreamJobImplES(esAddress, clusterName, indexName, typeName, routingKey, esBulkSize, esBulkMaxInterval);
	    } catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	} else {
	    job = new KafkaStreamJobImplStdout();
	}
	
	KafkaGroupedConsumer consumerGroup = new KafkaGroupedConsumer(zooKeeper, groupId, 1);
	List<KafkaStream<byte[], byte[]>> streams = consumerGroup.getMessageStreams(topic);
	ExecutorService executor = Executors.newFixedThreadPool(1);
	
	
	try {
	    executor.submit(new KafkaStreamHandler(streams.get(0), 0, job)).get();
	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
        	executor.shutdown();
        	consumerGroup.close();
	}
    }

    private static Map<String, String> parseCmdOptions(String[] args)
	    throws ParseException {
	Options options = buildOptions();
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
	String esAddress = cmd.getOptionValue(OPTION_ES_ADDRESS);
	String esClusterName = cmd.getOptionValue(OPTION_ES_CLUSTER_NAME);
	String esIndexName = cmd.getOptionValue(OPTION_ES_INDEX_NAME);
	String esTypeName = cmd.getOptionValue(OPTION_ES_TYPE_NAME);
	String esRoutingKeyName = cmd.getOptionValue(OPTION_ES_ROUTINGKEY);
	String esBulkSize = cmd.getOptionValue(OPTION_ES_BULKSIZE);
	String esBulkMaxIntervalSec = cmd.getOptionValue(OPTION_ES_BULK_INTERVAL_SEC);
	
	/* not implemented */
	String fetchSize = cmd.getOptionValue("fetchSize");
	String bulkInsertSize = cmd.getOptionValue("bulkSize");

	map.put(OPTION_ZOOKEEPER, zkAddress);
	map.put(OPTION_GROUP, groupId);
	map.put(OPTION_TOPIC, topic);
	map.put(OPTION_ES_ADDRESS, esAddress);
	map.put(OPTION_ES_CLUSTER_NAME, esClusterName);
	map.put(OPTION_ES_INDEX_NAME, esIndexName);
	map.put(OPTION_ES_TYPE_NAME, esTypeName);
	map.put(OPTION_ES_ROUTINGKEY, esRoutingKeyName);
	map.put(OPTION_ES_BULKSIZE, esBulkSize);
	map.put(OPTION_ES_BULK_INTERVAL_SEC, esBulkMaxIntervalSec);
	
	if(cmd.hasOption(OPTION_ENABLE_ES_INSERT) && esAddress != null && esClusterName != null) {
	    map.put(OPTION_ENABLE_ES_INSERT, "true");
	    map.put(OPTION_ES_ADDRESS, esAddress);
	    map.put(OPTION_ES_CLUSTER_NAME, esClusterName);
	} else if(cmd.hasOption(OPTION_ENABLE_ES_INSERT)) {
	    HelpFormatter formatter = new HelpFormatter();
	    formatter.printHelp("kafka2es --enable --elasticsearch=<urls> -- clustername=<name> ...", options);
	    return null;
	} else {
	    map.put(OPTION_ENABLE_ES_INSERT, "false");
	}
	
	return map;
    }

    private static Options buildOptions() {
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
	Option enableES = OptionBuilder.withArgName("required action with data")
		.withDescription("enable inserting data to elasticsearch")
		.create(OPTION_ENABLE_ES_INSERT);
	@SuppressWarnings("static-access")
	Option esAddress = OptionBuilder.withArgName("required action with data").hasArg()
		.withArgName("urls")
		.withDescription("The connection string for the elasticsearch connection in the form host:port. Multiple URLS can be given to allow fail-over.")
		.create(OPTION_ES_ADDRESS);
	@SuppressWarnings("static-access")
	Option esClusterName = OptionBuilder.withArgName("required action with data").hasArg()
		.withArgName("boolean")
		.withDescription("enable inserting data to elasticsearch")
		.create(OPTION_ES_CLUSTER_NAME);
	@SuppressWarnings("static-access")
	Option esIndex = OptionBuilder
		.withArgName("index of es data")
		.hasArg() 
		.withDescription(
				"index of es data")
		.create(OPTION_FETCH_SIZE);
	@SuppressWarnings("static-access")
	Option esType = OptionBuilder
		.withArgName("type of es data")
		.hasArg()
		.withDescription(
				"type of es data")
		.create(OPTION_FETCH_SIZE);
	@SuppressWarnings("static-access")
	Option fetchSize = OptionBuilder
		.withArgName("urls")
		.hasArg()
		.withDescription(
			"The amount of data to fetch in a single request. (default: 1048576)")
		.create(OPTION_FETCH_SIZE);
	@SuppressWarnings("static-access")
	Option fetchSize = OptionBuilder
		.withArgName("urls")
		.hasArg()
		.withDescription(
			"The amount of data to fetch in a single request. (default: 1048576)")
		.create(OPTION_FETCH_SIZE);
	@SuppressWarnings("static-access")
	Option fetchSize = OptionBuilder
		.withArgName("urls")
		.hasArg()
		.withDescription(
			"The amount of data to fetch in a single request. (default: 1048576)")
		.create(OPTION_FETCH_SIZE);
	@SuppressWarnings("static-access")
	Option fetchSize = OptionBuilder
		.withArgName("urls")
		.hasArg()
		.withDescription(
			"The amount of data to fetch in a single request. (default: 1048576)")
		.create(OPTION_FETCH_SIZE);
	@SuppressWarnings("static-access")
	Option fetchSize = OptionBuilder
		.withArgName("urls")
		.hasArg()
		.withDescription(
			"The amount of data to fetch in a single request. (default: 1048576)")
		.create(OPTION_FETCH_SIZE);
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
	options.addOption(enableES);
	options.addOption(esAddress);
	options.addOption(esClusterName);
	return options;
    }
}