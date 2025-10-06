package io.jana.demos.kafka;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());

    public static void main(String[] args) {


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","PLAINTEXT://172.31.23.120:9092");
        // properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("batch.size","5");
       // properties.setProperty("partitioner.class",org.apache.kafka.clients.producer.RoundRobinPartitioner.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        for(int i=0;i<10;i++){
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("toyys","barbienew "+i);


            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        logger.info("Sent Successfully into the Topic {} Offset {} Partition {}", recordMetadata.topic(), recordMetadata.offset(), recordMetadata.partition());
                    }
                }
            });

        }



        kafkaProducer.flush();

        kafkaProducer.close();
    }
}
