package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello world!");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties())) {
            for (int j = 0; j < 10; j++) {
                for (int i = 0; i < 30; i++) {
                    produceRecord(i, producer);
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            producer.flush();
        }
    }

    private static void produceRecord(int i, KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", String.format("hello world: %d", i));
        producer.send(producerRecord, (metadata, e) -> {
            if (e == null) {
                log.info(String.format("Metadata:\nTopic: %s\nPartition: %d\nOffest: %d\nTimestamp: %d",
                        metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()));
            } else {
                log.error("Exception while producing", e);
            }
        });
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        return properties;
    }
}
