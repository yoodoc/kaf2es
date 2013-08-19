package com.ktcloudware.kafka2es.kafkastream;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaStreamJobImplESTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {
	try {
	    Properties properties = new Properties();
	    properties.setProperty("indexName", "yoodoc0819");
	    properties.setProperty("typeName", "yoodoc");
	    KafkaStreamJobImplES streamJob = new KafkaStreamJobImplES("14.63.214.195:15930", "elasticsearch");
	    streamJob.excute("{\"testdata\":\"data\"}", properties);
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

}
