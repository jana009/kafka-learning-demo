package io.jana.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeysCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeysCallback.class.getName());

    public static void main(String[] args) {


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","PLAINTEXT://172.31.23.120:9092");
        // properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("batch.size","5");

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);
        String topic = "toyys";


        for(int j=0;j<=1;j++){

            for(int i=0;i<10;i++){

                String key = "id "+i;
                String value  = "barbie "+i;


                ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,key,value);


                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e==null){
                            logger.info("Sent Successfully into the Topic {} Offset {} Key {} | Partition {}", recordMetadata.topic(), recordMetadata.offset(), key, recordMetadata.partition());
                        }
                    }
                });

            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }


        }

        kafkaProducer.flush();

        kafkaProducer.close();
    }
}
