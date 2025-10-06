//package io.jana.demos.kafka.opensearch;
//
//import org.apache.hc.client5.http.auth.AuthScope;
//import org.apache.hc.client5.http.auth.CredentialsProvider;
//import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
//import org.apache.hc.client5.http.impl.DefaultConnectionKeepAliveStrategy;
//import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
//import org.apache.hc.core5.http.HttpHost;
//import org.opensearch.action.index.IndexRequest;
//import org.opensearch.action.index.IndexResponse;
//import org.opensearch.client.RequestOptions;
//import org.opensearch.client.RestClient;
//import org.opensearch.client.RestHighLevelClient;
//import org.opensearch.client.indices.CreateIndexRequest;
//import org.opensearch.client.indices.CreateIndexResponse;
//import org.opensearch.common.unit.TimeValue;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.net.URI;
//import java.util.HashMap;
//
//public class OpenSearchConsumer {
//
//    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
//
//    public static RestHighLevelClient createOpenSearchClient() {
//        String connString = "http://localhost:9200";
////        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";
//
//        // we build a URI from the connection string
//        RestHighLevelClient restHighLevelClient;
//        URI connUri = URI.create(connString);
//        // extract login information if it exists
//        String userInfo = connUri.getUserInfo();
//
//        if (userInfo == null) {
//            // REST client without security
//            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("http",connUri.getHost(), connUri.getPort())));
//
//        } else {
//            // REST client with security
//            String[] auth = userInfo.split(":");
//
//            BasicCredentialsProvider cp = new BasicCredentialsProvider();
//            cp.setCredentials(new AuthScope(connUri.getHost(),connUri.getPort()), new UsernamePasswordCredentials(auth[0], auth[1].toCharArray()));
//
//            restHighLevelClient = new RestHighLevelClient(
//                    RestClient.builder(new HttpHost(connUri.getScheme(),connUri.getHost(), connUri.getPort()))
//                            .setHttpClientConfigCallback(
//                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
//                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
//
//
//        }
//
//        return restHighLevelClient;
//    }
//
//    public static void main(String[] args) {
//
//
//
//
//        // first create an OpenSearch Client
//        RestHighLevelClient openSearchClient = createOpenSearchClient();
//        log.info("Created client successfully");
//
////        CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
////        try(openSearchClient) {
////            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
////        } catch (IOException e) {
////            throw new RuntimeException(e);
////        }
//
//
//        CreateIndexRequest createIndexRequest = new CreateIndexRequest("custom-index");
////        createIndexRequest.settings(Settings.builder() //Specify in the settings how many shards you want in the index.
////                .put("index.number_of_shards", 4)
////                .put("index.number_of_replicas", 3)
////        );
//        //Create a set of maps for the index's mappings.
//        HashMap<String, String> typeMapping = new HashMap<String,String>();
//        typeMapping.put("type", "integer");
//        HashMap<String, Object> ageMapping = new HashMap<String, Object>();
//        ageMapping.put("age", typeMapping);
//        HashMap<String, Object> mapping = new HashMap<String, Object>();
//        mapping.put("properties", ageMapping);
//        createIndexRequest.mapping(mapping);
//        try {
//            CreateIndexResponse createIndexResponse = openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        //Adding data to the index.
//        IndexRequest request = new IndexRequest("custom-index"); //Add a document to the custom-index we created.
//        request.id("1"); //Assign an ID to the document.
//
//        HashMap<String, String> stringMapping = new HashMap<String, String>();
//        stringMapping.put("message:", "Testing Java REST client");
//        request.source(stringMapping); //Place your content into the index's source.
//        try {
//            IndexResponse indexResponse = openSearchClient.index(request, RequestOptions.DEFAULT);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        //create our kafka client
//
//        //main code logic
//
//        //close things
//
//
//
//    }
//}
