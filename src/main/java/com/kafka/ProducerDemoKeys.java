package com.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //this will get a logger for put class
        final Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers="127.0.0.1:9092";

        Properties properties=new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        for(int i=0;i<=10;i++){
            //while creating the producer record,we should keep in mind that the producer record has the key.
            String topic="first_topic";
            String value="hello World "+Integer.toString(i);
            String key="Key "+Integer.toString(i);

            logger.info("Key" +key);
            ProducerRecord<String,String> record=new ProducerRecord<String, String>(topic,key,value);

            //here apard from record we can also provide a callback
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is successfully sent or an exception occurs
                    if(e==null){
                        //the record was sent
                        logger.info("Received message \n"+
                                "Topic : " +recordMetadata.topic()+"\n"+
                                "offset : "+recordMetadata.offset()+"\n"+
                                "Topic : " +recordMetadata.partition()+"\n");
                    }else{

                    }
                }
            }).get();//here we are blocking it and making it as synchronous
        }

        //to send the data
        producer.flush();
        producer.close();
    }
}
