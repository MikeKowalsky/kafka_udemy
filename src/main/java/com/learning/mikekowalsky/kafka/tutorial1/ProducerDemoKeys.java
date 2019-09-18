//package com.learning.mikekowalsky.kafka.tutorial1;
//
//import org.apache.kafka.clients.producer.*;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Properties;
//
//public class ProducerDemoKeys {
//
//    public static void main(String[] args) {
//
//        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
//
//        final String bootstrapServers = "127.0.0.1:9092";
//
//        // create producers properties
//        Properties properties = new Properties();
////        properties.setProperty("bootstrap.servers", bootstrapServers);
////        // ----> how to serialize to bytes
////        properties.setProperty("key.serializer", StringSerializer.class.getName());
////        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//
//        // create the producer
//        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
//
//
//        for(int i=0; i<10; i++) {
//
//            String topic = "firstTopic";
//            String value = "hallo world --> " + i + "_" + i + "_" + i*2;
//            String key = "is_" + i;
//
//
//            //create producer record
//            ProducerRecord<String, String> producerRecord =
//                    new ProducerRecord<String, String>(topic, key, value);
//
//            // send data - asynchronous
//            producer.send(producerRecord, new Callback() {
//                public void onCompletion(RecordMetadata metadata, Exception exception) {
//
//                    //executes every time a record is successfully send or throw an exception
//                    if (exception == null) {
//                        // successfully send
//                        logger.info("Received new metadata. \n" +
//                                "Topic:  " + metadata.topic() + "\n" +
//                                "Partition:  " + metadata.partition() + "\n" +
//                                "Offset:  " + metadata.offset() + "\n" +
//                                "Timestamp:  " + metadata.timestamp() + "\n");
//                    } else {
//                        logger.error("Exception while producing " + exception);
//                    }
//
//                }
//            });
//
//        }
//
//        //flush data
//        producer.flush();
//        //flush and close producer
//        producer.close();
//
//    }
//}
