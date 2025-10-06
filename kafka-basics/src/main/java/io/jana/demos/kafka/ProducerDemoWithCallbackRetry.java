package io.jana.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbackRetry {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbackRetry.class.getName());

    public static void main(String[] args) {


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","PLAINTEXT://172.31.23.120:9092");
        // properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("batch.size","5");
       // properties.setProperty("partitioner.class",org.apache.kafka.clients.producer.RoundRobinPartitioner.class.getName());

        //set safe producer configs (kafka <=2.8)

//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
//

        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        for(int i=0;i<10;i++){
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("retrytopic","barbienew "+i);


            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        logger.info("Sent Successfully into the Topic {} Offset {} Partition {} Value {} Key {}",
                                recordMetadata.topic(), recordMetadata.offset(), recordMetadata.partition(),
                                producerRecord.value(),producerRecord.key());
                    }
                }
            });

        }



        kafkaProducer.flush();

        kafkaProducer.close();
    }
}
