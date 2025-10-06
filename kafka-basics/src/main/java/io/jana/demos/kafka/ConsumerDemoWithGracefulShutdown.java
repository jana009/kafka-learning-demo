package io.jana.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithGracefulShutdown {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithGracefulShutdown.class.getName());

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","PLAINTEXT://172.31.23.120:9092");
       // properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id","my-java-toy-group");
        properties.put("enable.auto.commit", "true");
        properties.setProperty("auto-offset-reset","earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());


        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
        consumer.subscribe(List.of("toyys"));
        
        Thread t = Thread.currentThread();


        
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                consumer.wakeup();

                try{
                    t.join();
                }catch (Exception e){
                    logger.info("Exception in main method");
                    e.printStackTrace();
                }
            }
        });

        try{
            while(true){
                logger.info("polling");
                ConsumerRecords<String, String> consumerRecords =  consumer.poll(Duration.ofMillis(1000));

                consumerRecords.forEach(record->{
                    logger.info("Key {} Value{} Partitions {} Offsets{} ",record.key(),record.value(),record.partition(),record.offset());
                });

            }
        } catch (WakeupException ex){
            logger.info("Shutting Gracefully !");
        }catch (Exception e) {
            logger.error("Exception in the kafka");
        }finally {
            consumer.close();
        }





    }
}
