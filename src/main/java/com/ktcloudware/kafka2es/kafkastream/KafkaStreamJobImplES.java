/**
 * @author yoodoc@gmail.com
 */

package com.ktcloudware.kafka2es.kafkastream;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.ktcloudware.kafka2es.esclient.ESBulkRequsetHandler;

public class KafkaStreamJobImplES implements KafkaStreamJob {
    private Client client;
    private String esAddress;
    private String clusterName;
    private BulkRequestBuilder bulkRequestBuilder;
    private int insertingDataCount = 0;
    private String typeName;
    private String indexName;
    private String routingKeyName;
    private int maxReqeustIntervalSec = 10;
    private int bulkRequestSize = 1;
    private long lastSendTime;
    private Pattern routingKeyPattern;

    /**
     * 
     * @param esAddress
     *            : elastic search의 주소
     * @param clusterName
     *            : elastic search cluster 이름
     * @param index
     * @param type
     * @param routingKey
     *            : null일 경우 routing key를 설정하지 않는다.
     * @param bulkRequestSize
     *            : 한번에 보낼 es data의 수
     * @param maxRequestIntervalSec
     *            : 최대 bulk insert 대기 시간
     * @throws Exception
     */
    public KafkaStreamJobImplES(String esAddress, String clusterName,
	    String index, String type, String routingKey, int bulkRequestSize,
	    int maxRequestIntervalSec) throws Exception {
	this.esAddress = esAddress;
	this.clusterName = clusterName;
	this.indexName = index;
	this.typeName = type;
	this.routingKeyName = routingKey;
	this.bulkRequestSize = (bulkRequestSize > 0) ? bulkRequestSize : 1;
	this.maxReqeustIntervalSec = (maxRequestIntervalSec > 0) ? maxRequestIntervalSec
		: 10;
	openESConnection();
	lastSendTime = System.currentTimeMillis();
	this.setRoutingKeyPattern(Pattern.compile("\"" + routingKeyName
		+ "\"[\\s]*:[\\s]*\"([^\"]+)\""));
    }

    /**
     * properties는 다음과 같다. indexName="yoodoc" : ElasticSearch의 index 이름
     * typeName="vm" : 저장될 data의 type 이름
     */
    @Override
    public synchronized KafkaStreamJobResult excute(String data) {
	Settings settings = ImmutableSettings.settingsBuilder()
		.put("cluster.name", clusterName).build();
	
	appendBulkRequestBuilder(this.indexName, this.typeName,
		data);
	insertingDataCount++;
	long currentTime = System.currentTimeMillis();
	if (insertingDataCount > bulkRequestSize
		|| (currentTime - lastSendTime) > maxReqeustIntervalSec * 1000) {
	    // insert data To ElasticSearch
	    sendBulkRequest();
	    insertingDataCount = 0;
	}

	KafkaStreamJobResult jobResult = new KafkaStreamJobResult();

	return jobResult;
    }

    @Override
    public KafkaStreamJobResult excute(String data, Properties properties) {
	return excute(data);
    }

    private void appendBulkRequestBuilder(String indexName, String typeName,
	    String data) {

	IndexRequestBuilder source = null;
	if (this.routingKeyName == null) {
	    source = client.prepareIndex(indexName, typeName).setSource(data);
	} else {
	    Matcher matcher = getRoutingKeyPattern().matcher(data);
	    if (matcher.find()) {
		source = client.prepareIndex(indexName, typeName)
			.setSource(data).setRouting(matcher.group(1));
		System.out.println(matcher.group(1));
	    }
	}
	this.bulkRequestBuilder.add(source);
    }

    public String getClusterName() {
	return clusterName;
    }

    public void setClusterName(String clusterName) {
	this.clusterName = clusterName;
    }

    private synchronized void openESConnection() throws Exception {
	String[] esAddress = this.esAddress.split(",");
	int port;

	Settings settings = ImmutableSettings.settingsBuilder()
		.put("cluster.name", clusterName).build();
	TransportClient transportClient = new TransportClient(settings);
	for (String address : esAddress) {
	    String[] ipAndPort = address.split(":");
	    System.out.println(ipAndPort[0]);
	    System.out.println(ipAndPort[1]);
	    if (ipAndPort.length == 2) {
		port = Integer.valueOf(ipAndPort[1]);
		
		this.client = transportClient .addTransportAddress(new
		InetSocketTransportAddress( ipAndPort[0], port));
		 
	    } else {
		throw new Exception("wrong elasticsearch address format");
	    }
	}
	this.bulkRequestBuilder = client.prepareBulk();
    }

    private boolean sendBulkRequest() {
	try {

	    BulkResponse bulkResponse = this.bulkRequestBuilder.execute()
		    .actionGet();
	    if (bulkResponse.hasFailures()) {
		System.out.println("bulk insert fail: "
			+ bulkResponse.buildFailureMessage());
		return false;
	    } else {
		System.out.println("bulk insert took "
			+ bulkResponse.getTookInMillis() + "ms");
	    }
	} catch (ElasticSearchException e) {
	    e.printStackTrace();
	}
	this.bulkRequestBuilder = client.prepareBulk();
	lastSendTime = System.currentTimeMillis();
	insertingDataCount = 0;
	return true;
    }

    @Override
    public void shutdown() {
	sendBulkRequest();

	bulkRequestBuilder = null;
	client.close();
	client = null;
    }

    public Pattern getRoutingKeyPattern() {
	return routingKeyPattern;
    }

    public void setRoutingKeyPattern(Pattern routingKeyPattern) {
	this.routingKeyPattern = routingKeyPattern;
    }

}
