package com.javier_cloud.demos.streaming.util;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class AppProperties {
    private static final Logger LOG = LoggerFactory.getLogger(AppProperties.class);

    private static Properties props = null;

    public static Properties loadProperties(StreamExecutionEnvironment env) throws IOException {
        Map<String, Properties> applicationProperties;

        if (env instanceof LocalStreamEnvironment) {
            applicationProperties  = KinesisAnalyticsRuntime.getApplicationProperties(Objects.requireNonNull(AppProperties.class.getClassLoader().getResource("ApplicationProperties.json")).getPath());

        } else {
            applicationProperties  = KinesisAnalyticsRuntime.getApplicationProperties();
        }
        if (applicationProperties.get("FlinkApplicationProperties") == null) {
            LOG.warn("Unable to load FlinkApplicationProperties properties");
        }

        props = applicationProperties.get("FlinkApplicationProperties");
        return props;
    }

    public static String getBootstrapServers() {
        return props.getProperty("bootstrap.servers", "localhost:9092");
    }

    public static String getInputStream() {
        return props.getProperty("input_stream", "flink_input");
    }

    public static String getOutputStream() {
        return props.getProperty("output_stream", "flink_output");
    }

    public static String getGroupId() {
        return props.getProperty("group.id", "KafkaStreamingDemo");
    }

    public static String getESEndpoint() {
        return props.getProperty("elasticsearch_endpoint", "http://localhost:9200");
    }

    public static Boolean getESAnonymous() {
        return props.getProperty("elasticsearch_anonymous", "false").equalsIgnoreCase("true");
    }

    public static String getESUser() {
        String user = props.getProperty("elasticsearch_user");
        if (user == null || user.isEmpty()) {
            user = System.getenv("ELASTICSEARCH_USER");
        }
        return user;
    }

    public static String getESPassword() {
        String password = props.getProperty("elasticsearch_password");
        if (password == null || password.isEmpty()) {
            password = System.getenv("ELASTICSEARCH_PASSWORD");
        }
        return password;
    }

    public static String getESWordCountIndex() {
        return props.getProperty("elasticsearch_word_count_index", "word_count");
    }

    public static String getAWSRegion() {
        return props.getProperty("aws.region", "eu-west-1");
    }
}
