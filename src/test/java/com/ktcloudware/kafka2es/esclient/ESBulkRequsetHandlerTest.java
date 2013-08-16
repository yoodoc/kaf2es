package com.ktcloudware.kafka2es.esclient;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;

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

public class ESBulkRequsetHandlerTest {
	TransportClient mockESClient;
	BulkRequestBuilder mockBulkReqeustBuilder;
	ESBulkRequsetHandler esBulkRequsetHandler;
	
	@Before
	public void setUp() throws Exception {
		mockESClient = createMock(TransportClient.class);
		mockBulkReqeustBuilder = createMock(BulkRequestBuilder.class);
		esBulkRequsetHandler = new ESBulkRequsetHandler("localhost", 9300, "elasticsearch"){
			boolean executeBulkReqeust(BulkRequestBuilder bulkRequest) {
				return true;
			}
		};
		esBulkRequsetHandler.setClient((Client) mockESClient);
	}

	@After
	public void tearDown() throws Exception {
		mockESClient = null;
		mockBulkReqeustBuilder = null;
		esBulkRequsetHandler = null;
	}

	@Test
	public void testConstructor()
	{
		//mockESClient = createMock(TransportClient.class);
		replay(mockESClient);
		esBulkRequsetHandler = null;
		esBulkRequsetHandler = new ESBulkRequsetHandler("localhost", 9300, "es");
		verify(mockESClient);
	}
	
	@Test
	public void testClose()
	{
		//mockESClient = createMock(TransportClient.class);
		//adding behavior
		mockESClient.close();
		expectLastCall().times(1);
		expectLastCall().asStub();
		
		//switch the Mock Object to replay state.
		replay(mockESClient);
		
		//excute method 
		mockESClient.close();
		
		//Verifying Mock Behavior
		verify(mockESClient);
	
		//assert
	}
	

	@Test
	public void testInsert()
	{
		//create mock 
		IndexRequestBuilder mockIndexRequestBuilder = createMock(IndexRequestBuilder.class); 
		//mockESClient = createMock(TransportClient.class);
		
		//adding behavior
		expect(mockIndexRequestBuilder.setSource(anyObject(String.class))).andReturn(mockIndexRequestBuilder);
		expect(mockBulkReqeustBuilder.add(mockIndexRequestBuilder)).andReturn(null);
		expect(mockESClient.prepareBulk()).andReturn(mockBulkReqeustBuilder);
		expect(mockESClient.prepareIndex("indexName", "typeName")).andReturn(mockIndexRequestBuilder);
		
		//switch the Mock Object to replay state.
		replay(mockIndexRequestBuilder);
		replay(mockBulkReqeustBuilder);
		replay(mockESClient);
		
		//excute method
		List<String> data = new ArrayList<String>();
		data.add("data");
		esBulkRequsetHandler.insert("indexName", "typeName", data);
		
		//Verifying Mock Behavior
		verify(mockESClient);
	
		//assert
	}

	private String data() {
		// TODO Auto-generated method stub
		return null;
	}

}
