package com.javier_cloud.demos.streaming.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.*;

public class ESSinkBuilder {
    private static transient Gson gson = new GsonBuilder().create();

    public static <T> ElasticsearchSink<T> buildElasticSearchSink(String index) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(HttpHost.create(AppProperties.getESEndpoint()));

        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<T>() {
                    public IndexRequest createIndexRequest(T element) {
                        return Requests.indexRequest()
                                .index(index)
                                .type(index)
                                .source(gson.toJson(element), XContentType.JSON);
                    }

                    @Override
                    public void process(T element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        esSinkBuilder.setBulkFlushMaxActions(1);

        if (!AppProperties.getESAnonymous()) {
            /* if your elasticsearch needs authentication */

            esSinkBuilder.setRestClientFactory(
                    restClientBuilder -> {
                        restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(AppProperties.getESUser(), AppProperties.getESPassword()));

                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        });
                    }
            );
        }


        return esSinkBuilder.build();
    }
}
