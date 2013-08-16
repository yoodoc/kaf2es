package com.ktcloudware.kafka2es.esclient;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESBulkRequsetHandler {
    protected Client client;

    public ESBulkRequsetHandler(String address, int port, String clusterName) {
	open(address, port, clusterName);
    }

    public boolean insert(String indexName, String typeName, String data) {
	ArrayList<String> dataList = new ArrayList<String>();
	dataList.add(data);
	return insert(indexName, typeName, dataList);
    }

    public boolean insert(String indexName, String typeName, List<String> data) {
	BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
	for (String datum : data) {
	    try {
		IndexRequestBuilder source = client.prepareIndex(indexName,
			typeName).setSource(datum);
		if (source == null)
		    System.out.println("!!!!!!");
		bulkRequestBuilder.add(source);
	    } catch (Exception e) {
		// TODO Auto-generated catch block
		System.out.println("error at " + datum);
		e.printStackTrace();
	    }
	}

	// execute bulk insert
	return executeBulkReqeust(bulkRequestBuilder);
    }

    public Client getClient() {
	return client;
    }

    public void setClient(Client client) {
	this.client = client;
    }

    void open(String address, int port, String clusterName) {
	Settings settings = ImmutableSettings.settingsBuilder()
		.put("cluster.name", clusterName).build();
	TransportClient transportClient = new TransportClient(settings);
	this.client = transportClient
		.addTransportAddress(new InetSocketTransportAddress(address,
			port));
    }

    boolean executeBulkReqeust(BulkRequestBuilder bulkRequest) {
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
