package io.jana.projects.kafka.wikimedia;


import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WikimediaChangeHandler implements BackgroundEventHandler {

 private static final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getName());


    KafkaProducer<String,String> kafkaProducer;
    String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {

        logger.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {

    }
}
