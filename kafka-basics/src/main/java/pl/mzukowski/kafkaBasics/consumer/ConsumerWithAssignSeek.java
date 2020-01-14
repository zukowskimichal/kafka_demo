package pl.mzukowski.kafkaBasics.consumer;

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWithAssignSeek {

    public final static String TOPIC = "test.topic";

    public ConsumerWithAssignSeek() {

    }

    public static void main(String[] args) {
        new ConsumerWithAssignSeek().run();
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerWithAssignSeek.class.getName());
        String groupId = "my.application.3";
        String offsetReset = "earliest";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);

        //Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are used to replay data or fetch a specific message : )
        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC, 0);
        consumer.assign(ImmutableList.of(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, 15);

        int numberOfMessagesToRead = 5;
        boolean keepOneReading = true;

        while (keepOneReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord record : records) {
                numberOfMessagesToRead -= 1;
                logger.info("key: {}", record.key());
                logger.info("value: {}", record.value());
                logger.info("partition: {}", record.partition());
                logger.info("offset: {}", record.offset());
                if (numberOfMessagesToRead == 0) {
                    keepOneReading = false;
                    break;
                }
            }

        }
        logger.info("Terminating....");
    }

}
