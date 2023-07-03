package org.example;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class WikimediaChangeHandler implements BackgroundEventHandler {

    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        log.info("onOpen");
    }

    @Override
    public void onClosed() {
        log.info("onClosed");
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        String data = messageEvent.getData();
        log.info(data);
        producer.send(new ProducerRecord<>(topic, data));
    }

    @Override
    public void onComment(String comment) {
        log.info("onComment: {}", comment);
    }

    @Override
    public void onError(Throwable t) {
        log.error("onError", t);
    }
}
