package pl.mzukowski.kafkaBasics.consumer;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWithThreads {

    public final static String TOPIC = "test.topic";

    public ConsumerWithThreads() {

    }

    public static void main(String[] args) {
        new ConsumerWithThreads().run();
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerWithThreads.class.getName());
        String groupId = "my.application.1";
        String offsetReset = "earliest";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);

        //PConsumers
        CountDownLatch latch = new CountDownLatch(1);
        //thread
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, TOPIC, "my.application.2");
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //shutdown hook
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> {
                    logger.info("Caught shutdown hook");
                    ((ConsumerRunnable) myConsumerRunnable).shutdown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("Application terminated");
                }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupt", e);
        } finally {
            logger.info("Application is closing ");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch latch, String topic, String groupId) {
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.latch = latch;
            this.consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(ImmutableSet.of(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record : records) {
                        logger.info("key: {}", record.key());
                        logger.info("value: {}", record.value());
                        logger.info("partition: {}", record.partition());
                        logger.info("offset: {}", record.offset());
                    }

                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
