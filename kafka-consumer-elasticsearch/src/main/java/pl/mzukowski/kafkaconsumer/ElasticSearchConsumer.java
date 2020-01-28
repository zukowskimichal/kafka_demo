package pl.mzukowski.kafkaconsumer;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.time.Duration;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient() {
        String hostname = "ealstic-bombastic-350228852.eu-central-1.bonsaisearch.net";
        String username = "6lyj88tz6r";
        String password = "evimp5hqky";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(
                        httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(
                                credentialsProvider));

        return new RestHighLevelClient(restClientBuilder);
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> kafkaConsumer = ConsumerGroups.createConsumer();
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> {
                    logger.info("closing application");
                    logger.info("closing client");
                    try {
                        client.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                        logger.error("error during closing");

                    } finally {
                        logger.info("client closed");

                    }
                    logger.info("Closing kafka client");
                    kafkaConsumer.close();
                    logger.info("Kafka client shut down ");
                }));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            int recordsCount = records.count();

            logger.info("Received : {}", recordsCount);

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                //twitter fees specific id:
                try {
                    String twitterId = extractIdFromTweet(record.value());
                    IndexRequest indexRequest = new IndexRequest("twitter").source(record.value(), XContentType.JSON,
                            "id", twitterId);
                    bulkRequest.add(indexRequest);

                } catch (RuntimeException ex) {
                    logger.warn("skipping bad data: {}", record.value(), ex);
                }
            }
            //sending to elasticsearch
            if (recordsCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing offset....");
                kafkaConsumer.commitSync();
                logger.info("Offset Committed : )");
            }
        }
    }

    private static String extractIdFromTweet(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
