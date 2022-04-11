package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoShutdown {
    public static void main(String[] args){
        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers="127.0.0.1:9092";
        String groupId="my-fifth-application";
        String topic ="first_topic";

        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);

        //get a reference to the current thread because the shutdown hook
        //will run in a different thread.
        final Thread mainThread=Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                logger.info("detected a shutdown,let's exit by calling the consumer.wakeup()...");
                consumer.wakeup();//when it is called consumer.poll() will throw exception.

                //join the main thread to allow the execution of the code in the main thread
                //don't start running this thread until the main thread completes.
                try{
                    mainThread.join();
                }catch(InterruptedException ex){
                    ex.printStackTrace();
                }
            }
        });

        //consumer.subscribe(Collections.singleton(topic));
        consumer.subscribe(Arrays.asList(topic));

        //after adding consumer.wakeup() consumer.poll will throw exception so we need to catch it
        try{
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

        }catch(WakeupException e){
            logger.info("Wakeup up exception");
            //we expect this so ignore
        }catch(Exception e){
            logger.error("unexpected exception");
        }finally {
            consumer.close();//this will also commit the offsets
            logger.info("the consumer is now close");
        }
    }
}
