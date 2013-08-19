/**
 * @author yoodoc@gmail.com
 */

package com.ktcloudware.kafka2es.kafkastream;

import java.util.Properties;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class KafkaStreamJobImplES implements KafkaStreamJob {
    private Client client;
    private String esAddress;
    private String clusterName;
    private Properties properties;
    //private BulkRequestBuilder bulkRequestBuilder;
    private int insertingDataCount = 0;
    private static final int INSERTING_SIZE = 0;
    private static final String PROPERTY_INDEX_NAME = "indexName";
    private static final String PROPERTY_TYPE_NAME = "typeName";

    /**
     * 
     * @param address
     *            : elastic search의 주소
     * @param port
     *            : elastic search의 포트
     * @param clusterName
     *            : elastic search cluster 이름
     * @throws Exception 
     */
    public KafkaStreamJobImplES(String esAddress, String clusterName) throws Exception {
	this.esAddress = esAddress;
	this.clusterName = clusterName;
	openESConnection();
    }

    @Override
    public void addProperties(Properties properties) {
	// TODO Auto-generated method stub

    }

    /**
     * properties는 다음과 같다. indexName="yoodoc" : ElasticSearch의 index 이름
     * typeName="vm" : 저장될 data의 type 이름
     */
    @Override
    public KafkaStreamJobResult excute(String data) {
	if (this.properties == null
		|| !this.properties.contains(PROPERTY_TYPE_NAME)
		|| !this.properties.contains(PROPERTY_INDEX_NAME)) {
	    properties = new Properties();
	    properties.setProperty("indexName", "yoodoc");
	    properties.setProperty("typeName", "vm");
	}

	return excute(data, properties);
    }

    /**
     * properties는 다음과 같다. indexName : ElasticSearch의 index 이름 typeName : 저장될
     * data의 type 이름
     */
    @Override
    public KafkaStreamJobResult excute(String data, Properties properties) {
	String indexName = (String) properties.get(PROPERTY_INDEX_NAME);
	String typeName = (String) properties.get(PROPERTY_TYPE_NAME);
	BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
	prefareInsert(bulkRequestBuilder, indexName, typeName, data);
	insertingDataCount++;
	
	//if (insertingDataCount > INSERTING_SIZE) {
	    //insert data To ElasticSearch
	    insert(bulkRequestBuilder);
	    
	    //refresh bulkRequestBuilder & count
	   // bulkRequestBuilder = client.prepareBulk();
	  //  insertingDataCount = 0;
	//}
	KafkaStreamJobResult jobResult = new KafkaStreamJobResult();

	return jobResult;
    }

    private void prefareInsert(BulkRequestBuilder requestBuilder,  String indexName, String typeName, String data) {
	IndexRequestBuilder source = client.prepareIndex(indexName, typeName)
		.setSource(data);
	requestBuilder.add(source);
    }

    public String getClusterName() {
	return clusterName;
    }

    public void setClusterName(String clusterName) {
	this.clusterName = clusterName;
    }

    private void openESConnection() throws Exception {
	String[] esAddress = this.esAddress.split(",");
	int port;
	Settings settings = ImmutableSettings.settingsBuilder()
		.put("cluster.name", clusterName).build();
	TransportClient transportClient = new TransportClient(settings);
	for(String address: esAddress){
	    String[] ipAndPort = address.split(":");
	    System.out.println(ipAndPort[0]);
	    System.out.println(ipAndPort[1]);
	    if (ipAndPort.length == 2) {
		port = Integer.valueOf(ipAndPort[1]);
		this.client = transportClient
			.addTransportAddress(new InetSocketTransportAddress(ipAndPort[0],port));
	    } else {
		throw new Exception("wrong elasticsearch address format");
	    }
	}
    }

    private boolean insert(BulkRequestBuilder bulkRequest) {
	try {
	    
	    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
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
	return true;
    }

}
