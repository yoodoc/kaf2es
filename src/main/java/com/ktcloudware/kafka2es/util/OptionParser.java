package com.ktcloudware.kafka2es.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.ktcloudware.kafka2es.Kafka2Es;

public class OptionParser {

    public static Map<String, String> parseCmdOptions(String[] args)
	    throws ParseException {
	Options options = OptionParser.buildOptions();
	CommandLineParser parser = new GnuParser();
	CommandLine cmd = parser.parse(options, args);

	if (!cmd.hasOption(Kafka2Es.OPTION_ZOOKEEPER)
		|| !cmd.hasOption(Kafka2Es.OPTION_GROUP)
		|| !cmd.hasOption(Kafka2Es.OPTION_TOPIC)) {
	    HelpFormatter formatter = new HelpFormatter();
	    formatter.printHelp("kafka2es [OPTION]...", options);
	    return null;
	}
	Map<String, String> map = new HashMap<String, String>();
	String zkAddress = cmd.getOptionValue(Kafka2Es.OPTION_ZOOKEEPER);
	String groupId = cmd.getOptionValue(Kafka2Es.OPTION_GROUP);
	String topic = cmd.getOptionValue(Kafka2Es.OPTION_TOPIC);
	String esAddress = cmd.getOptionValue(Kafka2Es.OPTION_ES_ADDRESS);
	String esClusterName = cmd
		.getOptionValue(Kafka2Es.OPTION_ES_CLUSTER_NAME);
	String esIndexName = cmd.getOptionValue(Kafka2Es.OPTION_ES_INDEX_NAME);
	String esTypeName = cmd.getOptionValue(Kafka2Es.OPTION_ES_TYPE_NAME);
	String esRoutingKeyName = cmd
		.getOptionValue(Kafka2Es.OPTION_ES_ROUTINGKEY);
	String esBulkSize = cmd.getOptionValue(Kafka2Es.OPTION_ES_BULKSIZE);
	String esBulkMaxIntervalSec = cmd
		.getOptionValue(Kafka2Es.OPTION_ES_BULK_INTERVAL_SEC);

	/* not implemented */
	String fetchSize = cmd.getOptionValue("fetchSize");

	map.put(Kafka2Es.OPTION_ZOOKEEPER, zkAddress);
	map.put(Kafka2Es.OPTION_GROUP, groupId);
	map.put(Kafka2Es.OPTION_TOPIC, topic);
	map.put(Kafka2Es.OPTION_ES_ADDRESS, esAddress);
	map.put(Kafka2Es.OPTION_ES_CLUSTER_NAME, esClusterName);
	map.put(Kafka2Es.OPTION_ES_INDEX_NAME, esIndexName);
	map.put(Kafka2Es.OPTION_ES_TYPE_NAME, esTypeName);
	map.put(Kafka2Es.OPTION_ES_ROUTINGKEY, esRoutingKeyName);
	map.put(Kafka2Es.OPTION_ES_BULKSIZE, esBulkSize);
	map.put(Kafka2Es.OPTION_ES_BULK_INTERVAL_SEC, esBulkMaxIntervalSec);

	if (cmd.hasOption(Kafka2Es.OPTION_ENABLE_ES_INSERT)
		&& esAddress != null && esClusterName != null) {
	    map.put(Kafka2Es.OPTION_ENABLE_ES_INSERT, "true");
	    map.put(Kafka2Es.OPTION_ES_ADDRESS, esAddress);
	    map.put(Kafka2Es.OPTION_ES_CLUSTER_NAME, esClusterName);
	} else if (cmd.hasOption(Kafka2Es.OPTION_ENABLE_ES_INSERT)) {
	    HelpFormatter formatter = new HelpFormatter();
	    formatter
		    .printHelp(
			    "kafka2es --enable --elasticsearch=<urls> -- clustername=<name> ...",
			    options);
	    return null;
	} else {
	    map.put(Kafka2Es.OPTION_ENABLE_ES_INSERT, "false");
	}

	System.out.println("paring cli options");
	for (Entry<String, String> entry : map.entrySet()) {
	    System.out.println(entry.getKey() + "=" + entry.getValue());
	}

	return map;
    }

    /**
     * 
     * @return
     */
    public static Options buildOptions() {
	@SuppressWarnings("static-access")
	Option listener = OptionBuilder
		.withArgName("urls").hasArg()
		.withDescription(
			"REQUIRED:  The connection string for the zookeeper connection in the form host:port. Multiple URLS can be given to allow fail-over.")
		.create(Kafka2Es.OPTION_ZOOKEEPER);
	@SuppressWarnings("static-access")
	Option group = OptionBuilder.withArgName("group name").hasArg()
		.withDescription("REQUIRED: The group id to consume on.")
		.create(Kafka2Es.OPTION_GROUP);
	@SuppressWarnings("static-access")
	Option topic = OptionBuilder.withArgName("topic name").hasArg()
		.withDescription("REQUIRED: The topic id to consume on.")
		.create(Kafka2Es.OPTION_TOPIC);
	@SuppressWarnings("static-access")
	Option enableES = OptionBuilder
		.withArgName("required action with data")
		.withDescription("enable inserting data to elasticsearch")
		.create(Kafka2Es.OPTION_ENABLE_ES_INSERT);
	@SuppressWarnings("static-access")
	Option esAddress = OptionBuilder
		.withArgName("required action with data").hasArg()
		.withArgName("urls")
		.withDescription(
			"The connection string for the elasticsearch connection in the form host:port. Multiple URLS can be given to allow fail-over.")
		.create(Kafka2Es.OPTION_ES_ADDRESS);
	@SuppressWarnings("static-access")
	Option esClusterName = OptionBuilder.withArgName("ES cluster name").hasArg()
		.withDescription("enable inserting data to elasticsearch")
		.create(Kafka2Es.OPTION_ES_CLUSTER_NAME);
	@SuppressWarnings("static-access")
	Option esIndex = OptionBuilder.withArgName("index of es data").hasArg()
		.withDescription("index of es data")
		.create(Kafka2Es.OPTION_ES_INDEX_NAME);
	@SuppressWarnings("static-access")
	Option esType = OptionBuilder.withArgName("type of es data").hasArg()
		.withDescription("type of es data")
		.create(Kafka2Es.OPTION_ES_TYPE_NAME);
	@SuppressWarnings("static-access")
	Option esRoutingKey = OptionBuilder
		.withArgName("routing key for es data").hasArg()
		.withDescription("routing key for es data")
		.create(Kafka2Es.OPTION_ES_ROUTINGKEY);
	@SuppressWarnings("static-access")
	Option esBulkSize = OptionBuilder.withArgName("size").hasArg()
		.withDescription("es bulk insert size ")
		.create(Kafka2Es.OPTION_ES_BULKSIZE);
	@SuppressWarnings("static-access")
	Option esBulkMaxIntervalSec = OptionBuilder.withArgName("seconds").hasArg()
		.withDescription("es bulk insert max interval in seconds")
		.create(Kafka2Es.OPTION_ES_BULK_INTERVAL_SEC);
	@SuppressWarnings("static-access")
	Option fetchSize = OptionBuilder
		.withArgName("urls").hasArg()
		.withDescription(
			"The amount of data to fetch in a single request. (default: 1048576)")
		.create(Kafka2Es.OPTION_ES_INDEX_NAME);

	Options options = new Options();
	options.addOption(listener);
	options.addOption(fetchSize);
	options.addOption(group);
	options.addOption(topic);
	options.addOption(enableES);
	options.addOption(esAddress);
	options.addOption(esClusterName);
	options.addOption(esIndex);
	options.addOption(esType);
	options.addOption(esRoutingKey);
	options.addOption(esBulkSize);
	options.addOption(esBulkMaxIntervalSec);

	return options;
    }

}
