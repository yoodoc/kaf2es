/**
 * @author yoodoc@gmail.com
 */

package com.ktcloudware.kafka2es.kafkastream;

import java.util.Properties;

public interface KafkaStreamJob {

    public KafkaStreamJobResult excute(String data); 
    
    public KafkaStreamJobResult excute(String data, Properties properties);
    
    public void addProperties(Properties properties);
    
}