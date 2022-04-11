package com.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args){
        //this will get a logger for put class
        final Logger logger=LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());
        String bootstrapServers="127.0.0.1:9092";

        Properties properties=new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic1","hello world 1");

        //here apart from record we can also provide a callback
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes everytime a record is successfully sent or an exception occurs
                if(e==null){
                    //the record was sent
                    logger.info("Received message \n"+
                            "Topic : " +recordMetadata.topic()+"\n"+
                            "offset : "+recordMetadata.offset()+"\n"+
                            "Partition : " +recordMetadata.partition()+"\n"+
                            "Timestamp : " +recordMetadata.timestamp()+"\n");

                }else{
                    logger.error("error while producing : ",e);
                }
            }
        });

        //to send the data
        producer.flush();
        producer.close();
    }
}
