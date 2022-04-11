package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args){
        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class.getName());
        logger.info("i am a kafka consumer");

        String bootstrapServers="127.0.0.1:9092";
        String groupId="my-fifth-application";
        String topic ="first_topic1";

        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //none :if no offsets are found then don't start
        //earliest :read from the very beginning of the topic
        //latest :read only from the latest commited offset.
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);

        //consumer.subscribe(Collections.singleton(topic));//this is to subscribe to one topic
        consumer.subscribe(Arrays.asList(topic));//subscribing to multiple topics.

        //poll for the new data
        while(true){
            logger.info("polling");
            //if you don't receive anything from kafka for 100ms then go to the next line of code.
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            //then we iterate through the number of records received.
            for(ConsumerRecord<String,String> rec:records){
                logger.info("Key : "+rec.key() +" / "+" Value "+rec.value());
                logger.info("Partition : "+rec.partition() +" / "+" Offset "+rec.offset());
            }
        }
    }
}
