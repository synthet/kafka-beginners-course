package org.example;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
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
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown...");
            kafkaConsumer.wakeup();

            // join the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try (openSearchClient; kafkaConsumer) {
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
                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        bulkRequest.add(indexRequest);
                    } catch (NullPointerException ex) {
                        log.error(ex.getMessage());
                    }
                }
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted {} records into {} index", bulkResponse.getItems().length, INDEX_NAME);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                kafkaConsumer.commitSync();
                log.info("Commit offsets");
            }
        } catch (WakeupException ex) {
            log.info("Shutdown Consumer...");
        } catch (Exception ex) {
            log.error("Unexpected exception in Consumer", ex);
        } finally {
            kafkaConsumer.close();
            log.info("Shutdown Consumer... Done");
        }
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
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
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        URI connUri = URI.create(connString);
        HttpHost httpHost = new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme());
        return new RestHighLevelClient(RestClient.builder(httpHost));
    }
}