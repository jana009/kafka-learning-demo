package io.jana.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class OpenConsumerOld {
    private static final Logger logger = LoggerFactory.getLogger(OpenConsumerOld.class.getSimpleName());


    public static RestHighLevelClient createOpenSearchClient() {

        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    public static void main(String[] args) {

        RestHighLevelClient client =createOpenSearchClient();


        KafkaConsumer<String,String> kafkaConsumer = createKafkaConsumer();

        try(client;kafkaConsumer) {



            boolean indexExists = client.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists){
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("The Wikimedia Index has been created!");
            } else {
                logger.info("The Wikimedia Index already exits");
            }


            while(true){
                logger.info("polling");
                ConsumerRecords<String, String> consumerRecords =  kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("Record Count "+consumerRecords.count());

                for(ConsumerRecord<String,String> consumerRecord:consumerRecords){

                    String id = extractId(consumerRecord.value());

                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(consumerRecord.value(), XContentType.JSON)
                            .id(id); //to tell consumer about the request type
                    client.index(indexRequest,RequestOptions.DEFAULT);
                    logger.info("Inserted 1");

                }


            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }



    }

    private static String extractId(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","PLAINTEXT://172.31.23.120:9092");
        // properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id","consumer-opensearch-demo");
        properties.put("enable.auto.commit", "true");
        properties.setProperty("auto-offset-reset","latest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
        consumer.subscribe(List.of("wikimedia.recentchange"));
        return consumer;

    }


}
