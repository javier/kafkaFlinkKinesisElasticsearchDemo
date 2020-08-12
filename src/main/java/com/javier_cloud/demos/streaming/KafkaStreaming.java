package com.javier_cloud.demos.streaming;

import com.javier_cloud.demos.streaming.util.AppProperties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class KafkaStreaming {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AppProperties.loadProperties(env);
        Properties kafkaProperties = new Properties();
        String kafka_servers = AppProperties.getBootstrapServers();
        kafkaProperties.setProperty("bootstrap.servers", kafka_servers);
        kafkaProperties.setProperty("group.id", AppProperties.getGroupId());

        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer011<>(AppProperties.getInputStream(), new SimpleStringSchema(), kafkaProperties));


        FlinkKafkaProducer011<String> streamSink = new FlinkKafkaProducer011<>(kafka_servers, AppProperties.getOutputStream(), new SimpleStringSchema());
        streamSink.setWriteTimestampToKafka(true);

        stream.addSink(streamSink);

        env.execute("Basic Flink Kafka Streaming");
    }
}
