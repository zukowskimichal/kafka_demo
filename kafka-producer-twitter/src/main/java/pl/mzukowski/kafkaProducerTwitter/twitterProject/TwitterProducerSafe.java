package pl.mzukowski.kafkaSimpleProject.twitterProject;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducerSafe {

    private static final String TOPIC = "twitter.topic";
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducerSafe.class);
    private static final List<String> terms = Lists.newArrayList("kafka", "bitcoin", "usa", "java", "sport");

    private TwitterProducerSafe() {

    }

    public static void main(String[] args) throws IOException {
        new TwitterProducerSafe().run();
    }

    private void run() throws IOException {
        logger.info("Application running");
        //create twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        //create twitter producer
        //kafka producer props
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //safe producer props
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");// kafka used in the rpoject
        //is greater >=1.1 so we can keep it as 5

        //high throughput settings at the expense of cpu usage and latency
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //shutdownhook
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> {
                    logger.info("stopping client");
                    twitterClient.stop();
                    logger.info("stopping producer");
                    kafkaProducer.close();
                }));
        //loop to to send tweets
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
            if (Objects.nonNull(msg)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, msg);
                kafkaProducer.send(record, (metadata, exception) -> {
                    if (Objects.nonNull(exception)) {
                        logger.error("Something goes wrong", exception);
                    } else {
                        logger.info("Msg sent to partition: {}", metadata.partition());
                        logger.info("Msg with offset: {}", metadata.offset());
                    }
                });
            }
        }
        logger.info("Application terminated");

    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) throws IOException {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);
        Properties props = loadProperties();

        Authentication hosebirdAuth = new OAuth1(props.getProperty("api.key"), props.getProperty("api.secret"),
                props.getProperty("access.token"), props.getProperty("access.secret"));

        return new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .build();                          // optional: use this if you want to process client events
    }

    private Properties loadProperties() throws IOException {
        try (InputStream input = getClass().getResourceAsStream("/application.properties")) {
            Properties prop = new Properties();
            // load a properties file
            prop.load(input);
            return prop;
        }
    }
}
