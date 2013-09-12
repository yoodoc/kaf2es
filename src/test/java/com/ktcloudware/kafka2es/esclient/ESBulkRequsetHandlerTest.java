package com.ktcloudware.kafka2es.esclient;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.AbstractListenableActionFuture;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class ESBulkRequsetHandlerTest {

    ESBulkRequsetHandler bulkRequsetHandler;
    TransportClient mockESClient;

    @Before
    public void setUp() throws Exception {
	bulkRequsetHandler = mock(ESBulkRequsetHandler.class);
	mockESClient = mock(TransportClient.class);

    }

    @Test
    public void testInsertLocal() {
	ESBulkRequsetHandler es = new ESBulkRequsetHandler("14.63.226.175", 9300, "cdp_dev_qa");
	es.insert("yoodoc", "test", "data");
    }
    
    @Test
    public void testInsert() {

	// create mock
	bulkRequsetHandler = new ESBulkRequsetHandler(null, 0, null) {
	    @Override
	    void open(String address, int port, String clusterName) {
		this.client = mockESClient;
	    }
	    @Override
	    boolean executeBulkReqeust(BulkRequestBuilder bulkRequest) {
		return true;
	    }
	};
	
	IndexRequestBuilder mockIndexRequestBuilder = mock(IndexRequestBuilder.class);
	BulkRequestBuilder mockBulkReqeustBuilder = mock(BulkRequestBuilder.class);
	
	// adding behavior
	when(mockESClient.prepareBulk()).thenReturn(mockBulkReqeustBuilder);
	when(mockESClient.prepareIndex(anyString(), anyString())).thenReturn(
		mockIndexRequestBuilder);
	when(mockIndexRequestBuilder.setSource(anyString())).thenReturn(
		mockIndexRequestBuilder);
	
	/*
	 * when(mockBulkReqeustBuilder.add(mockIndexRequestBuilder)).thenReturn(null
	 * ); when(mockESClient.prepareBulk()).thenReturn(null);
	 */
	// action
	List<String> data = new ArrayList<String>();
	data.add("data");
	bulkRequsetHandler.insert("indexName", "typeName", data);

	// Verifying Mock Behavior

	// verify(bulkRequsetHandler).executeBulkReqeust(mockBulkReqeustBuilder);
	verify(mockESClient).prepareBulk();
	verify(mockESClient).prepareIndex("indexName", "typeName");
	verify(mockIndexRequestBuilder).setSource(anyString());
	//verify(bulkRequsetHandler).insert("indexName", "typeName", data);

    }

    private String data() {
	// TODO Auto-generated method stub
	return null;
    }

}
