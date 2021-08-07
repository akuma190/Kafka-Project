package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args){
        String bootstrapServers="127.0.0.1:9092";
        //create producer properties
        Properties properties=new Properties();
//        properties.setProperty("bootstrap.servers",bootstrapServers);
        //key and values serializer lets you know what type of values are you sending
        //to kafka and how should it be converted to bytes,0's and 1's
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer",StringSerializer.class.getName());
        //but above is an old method to do the configutration.
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create a producer
        //key will be a string and the value will also be string
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        //send data
        //send takes producer record as the inputs so we need to take producer records as input.

        //creating producer record
        //key and value are topic and values
        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic","hello world");

        //it is asynchronous
        producer.send(record);

        //to send the data
        producer.flush();
        producer.close();
    }
}
