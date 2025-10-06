package io.jana.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemoConfig {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","PLAINTEXT://172.31.23.120:9092");
       // properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("retrytopic","barbie");


        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();

        kafkaProducer.close();


    }
}
