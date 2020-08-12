package com.javier_cloud.demos.streaming;


import com.javier_cloud.demos.streaming.util.AppProperties;
import com.javier_cloud.demos.streaming.util.ESSinkBuilder;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KinesisDataStreamsStreamingToES {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AppProperties.loadProperties(env);
        String kafka_servers = AppProperties.getBootstrapServers();

        Properties KinesisStreamsProperties = new Properties();
        KinesisStreamsProperties.setProperty(ConsumerConfigConstants.AWS_REGION, AppProperties.getAWSRegion());
        KinesisStreamsProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        DataStream<String> stream = env
              .addSource(new FlinkKinesisConsumer<>(AppProperties.getInputStream(), new SimpleStringSchema(), KinesisStreamsProperties));


        FlinkKafkaProducer011<String> streamSink = new FlinkKafkaProducer011<String>(kafka_servers, AppProperties.getOutputStream(), new SimpleStringSchema());
        streamSink.setWriteTimestampToKafka(true);

        stream.addSink(streamSink);

        // split up the lines in pairs (2-tuples) containing: (word,1), then sum
        DataStream<Tuple2<String, Integer>> counts =
                stream.flatMap(new Tokenizer()).keyBy(0).sum(1);


        counts.addSink(ESSinkBuilder.buildElasticSearchSink(AppProperties.getESWordCountIndex()));

        env.execute("Streaming from a Kinesis Data Stream, echoing the message to Kafka, and outputting aggregations to ElasticSearch");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
