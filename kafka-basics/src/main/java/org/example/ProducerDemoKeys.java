package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello world!");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties())) {
            for (int j = 0; j < 2; j++) {
                for (int i = 0; i < 10; i++) {
                    String topic = "demo_java";
                    String key = String.format("id_%d", i);
                    String value = String.format("hello world: %d", i);
                    produceRecord(producer, topic, key, value);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            producer.flush();
        }
    }

    private static void produceRecord(KafkaProducer<String, String> producer, String topic, String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
        producer.send(producerRecord, (metadata, e) -> {
            if (e == null) {
                log.info(String.format("Key: %s | Partition: %d", key, metadata.partition()));
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
