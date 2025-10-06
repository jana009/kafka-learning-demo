package wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.OkHttpClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.URI;
import java.util.Properties;

public class WikimediaProducerSample {

    public static void main(String[] args) throws Exception {


        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers","PLAINTEXT://172.31.23.120:9092");
        // properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        String topic ="wikimedia.recentchange";
        KafkaProducer<String,String> producer =new KafkaProducer<>(properties);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        // Create unsafe TrustManager (dev only!)
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {}
                    public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {}
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() { return new java.security.cert.X509Certificate[0]; }
                }
        };

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());

        OkHttpClient client = new OkHttpClient.Builder()
                .sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustAllCerts[0])
                .hostnameVerifier((hostname, session) -> true)
                .build();

        EventSource.Builder esBuilder = new EventSource.Builder(eventHandler,URI.create("https://stream.wikimedia.org/v2/stream/recentchange"))
                .client(client);



        try (EventSource bes = esBuilder.build()) {
            bes.start();
            Thread.sleep(60000); // Listen for 1 minute
        }

    }
}
