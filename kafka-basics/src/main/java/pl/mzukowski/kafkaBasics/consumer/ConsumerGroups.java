package pl.mzukowski.kafkaBasics.consumer;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerGroups {

    public final static String TOPIC = "test.topic";

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerGroups.class.getName());
        String groupId = "my.application.1";
        String offsetReset = "earliest";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);

        //PConsumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to topic
        consumer.subscribe(ImmutableSet.of(TOPIC));
        //loop to ask for data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("key: {}", record.key());
                logger.info("value: {}", record.value());
                logger.info("partition: {}", record.partition());
                logger.info("offset: {}", record.offset());
            }
        }
    }
}
