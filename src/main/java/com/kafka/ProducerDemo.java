package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log= LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args){
        log.info("I am a kafka producer");
        String bootstrapServers="127.0.0.1:9092";

        // Step 1 : create producer properties
        Properties properties=new Properties();
        //properties.setProperty("bootstrap.servers",bootstrapServers);

        //key and values serializer lets you know what type of values are you sending
        //to kafka and how should it be converted to bytes,0's and 1's
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer",StringSerializer.class.getName());


        //but above is an old method to do the configutration.Because we can make typos
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Step 2 : create a producer
        //this tells that both the key and the values of the kafka producer are string.
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);


        //Step 3 : send data
        //send takes producer record as the inputs so we need to take producer records as input.

        //creating producer record
        //key and value are topic and values
        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic1","hello world");

        //it is asynchronous
        //here we send the record through the producer.
        producer.send(record);


        //Step 4 : flush and close the producer
        //flush - asynchronous
        //the code running will be blocked here until all the data is sent.
        producer.flush();

        producer.close();
    }
}
