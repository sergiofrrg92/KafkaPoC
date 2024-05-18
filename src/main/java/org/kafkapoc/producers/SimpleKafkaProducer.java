package org.kafkapoc.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class SimpleKafkaProducer {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public SimpleKafkaProducer(String brokers, String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

    public void produce(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        this.producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.out.println(Arrays.toString(exception.getStackTrace()));
                exception.printStackTrace();
            } else {
                System.out.println("Sent message with key: " + key + " and value: " + value);
            }
        });
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) {
        SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost:9092", "test-topic");
        for (int i = 0; i < 10; i++) {
            producer.produce(""+i, String.format("%s", new java.util.Date()));
        }
        producer.close();
    }
}
