package org.example;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
    private static final String INDEX_NAME = "wikimedia";

    public static void main(String[] args) {
        try (RestHighLevelClient openSearchClient = createOpenSearchClient();
                KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer()) {
            GetIndexRequest getIndexRequest = new GetIndexRequest(INDEX_NAME);
            boolean indexExists = openSearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            } else {
                log.info("Index exists");
            }
            kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received {} record(s)", recordCount);
                for (ConsumerRecord<String, String> record : records) {
                    IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
                            .source(record.value(), XContentType.JSON);
                    IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    log.info("Inserted {} document into {} index", indexResponse.getId(), INDEX_NAME);
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        return new KafkaConsumer<>(getProperties());
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "consumer-opensearch-demo");
        properties.setProperty("auto.offset.reset", "latest");
        return properties;
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        URI connUri = URI.create(connString);
        HttpHost httpHost = new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme());
        return new RestHighLevelClient(RestClient.builder(httpHost));
    }
}