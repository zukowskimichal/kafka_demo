package pl.mzukowski.kafkaBasics.producer;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeys {

    private final static String TOPIC = "test.topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Producer class

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 1000; i++) {
            //Create a producer record

            String key = "id_" + i;
            String value = "message_" + i;
            logger.info("key: {}", key);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, key, value);
            //send message
            producer.send(record, (recordMetadata, e) -> {
                //executes every time records is successfully sent or Exception is thrown
                if (Objects.isNull(e)) {
                    logger.info("Received new metadata:\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Partition: " + recordMetadata.partition());
                } else {
                    logger.error("Error occurred during producing", e);
                }

            }).get();
        }

        //flush and close
        producer.flush();
        producer.close();
    }
}
