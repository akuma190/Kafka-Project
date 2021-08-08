package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args){
        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers="127.0.0.1:9092";
        String topic ="first_topic";

        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);

        //here we will not subscribe to topics

        //assign
        TopicPartition partitionToReadFrom=new TopicPartition(topic,0);
        long offsetToReadFrom=15L;//red from topic from this offset
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek : we need to specific offsets
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numberOfMessagesToRead=5;
        boolean keepOnReading=true;
        int numberOfMessages=0;
        //poll for the new data

        while(keepOnReading){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> rec:records){
                numberOfMessages=numberOfMessages+1;
                logger.info("Key : "+rec.key() +" / "+" Value "+rec.value());
                if(numberOfMessages>=numberOfMessagesToRead){
                    keepOnReading=false;
                }
            }
        }
    }
}
