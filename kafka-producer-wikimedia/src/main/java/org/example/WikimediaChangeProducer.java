package org.example;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangeProducer {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Hello world!");

        try (BackgroundEventSource eventSource = getEventSource()) {
            eventSource.start();
            TimeUnit.MINUTES.sleep(10);
        }
    }

    private static BackgroundEventSource getEventSource() {
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        return new BackgroundEventSource.Builder(getHandler(), new EventSource.Builder(URI.create(url))).build();
    }

    private static BackgroundEventHandler getHandler() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());
        String topic = "wikimedia.recentchange";
        return new WikimediaChangeHandler(producer, topic);
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE.toString());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(20));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        return properties;
    }
}