package org.example;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
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
        properties.setProperty("bootstrap.servers", bootstrapServer);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        return properties;
    }
}