package com.learning.mikekowalsky.kafka.tutorial1;

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
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    public ConsumerDemoWithThreads(){}

    private void run(){
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "firstTopic";

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating new consumer.");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(latch, topic, bootstrapServer, groupId);

        // create and start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();


        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook.");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited.");
        }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted.", e);
        } finally {
            logger.info("Closing application.");
        }
    }


    private class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch latch, String topic, String bootstrapServer, String groupId){
            this.latch = latch;

            // create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to out topic(s)
            consumer.subscribe(Arrays.asList(topic)); // gives an option to put more topics
        }

        @Override
        public void run() {
            try{
                // pull for new data
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record : records){
                        logger.info("Key " + record.key() + " . Value " + record.value());
                        logger.info("Partition " + record.partition() + " . Offset " + record.offset());
                    }
                }
            } catch (WakeupException e){
                logger.info("Received shutdown signal.");
            } finally {
                consumer.close();
                // tell out main code that we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            // wakeUp Method is to interrupt consumer.poll()
            // by throwing an exception WakeUpException
            consumer.wakeup();
        }
    }

}


