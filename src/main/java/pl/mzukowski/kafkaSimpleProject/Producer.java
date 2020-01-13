package pl.mzukowski.kafkaSimpleProject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public final static String TOPIC = "test.topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Producer class

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, "hello_world");
        //send message
        producer.send(record);

        //flush and close
        producer.flush();
        producer.close();
    }
}
