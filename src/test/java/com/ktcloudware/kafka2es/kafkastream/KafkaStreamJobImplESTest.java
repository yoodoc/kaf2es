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
    public void test() throws Exception {
	    KafkaStreamJobImplES streamJob = new KafkaStreamJobImplES("localhost:15930", "elasticsearch", "yoodoc", "virtualmachine", "vmname", 0, 0);
	    streamJob.excute("{\"vmname\" : \"data\"}");
	    streamJob.shutdown();
    }

}
