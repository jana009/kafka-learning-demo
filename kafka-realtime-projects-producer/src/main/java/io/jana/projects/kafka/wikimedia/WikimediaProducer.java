package io.jana.projects.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {

    public static SSLContext getUnsafeSSLContext() throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                    public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {return new java.security.cert.X509Certificate[0];}
                }
        };
        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        return sc;
    }

    public static void main(String[] args) throws InterruptedException {



        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers","PLAINTEXT://172.31.23.120:9092");
        // properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        String topic ="wikimedia.recentchange";
        KafkaProducer<String,String> producer =new KafkaProducer<>(properties);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";


        BackgroundEventHandler eventHandler =new WikimediaChangeHandler(producer,topic) ;
        BackgroundEventSource.Builder  eventSourceBuilder = new BackgroundEventSource.Builder(eventHandler,new EventSource.Builder(URI.create(url)));
        try (BackgroundEventSource backgroundEventSource = eventSourceBuilder.build()) {

            backgroundEventSource.start();
        }

        TimeUnit.MINUTES.sleep(5);

    }


}
