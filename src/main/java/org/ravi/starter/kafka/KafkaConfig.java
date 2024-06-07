package org.ravi.starter.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author raviteja.kothapalli
 */
public class KafkaConfig {
    public static final Map<String, Object> KAFKA_PARAMS;
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String GROUP_ID = "kafka-consumer-java";
    public static final String TOPIC = "kafka-local";

    static {
        final Map<String, Object> tmpMap = new HashMap<>();
        tmpMap.put("metadata.broker.list", KAFKA_BROKER);
        tmpMap.put("bootstrap.servers", KAFKA_BROKER);
        tmpMap.put("key.deserializer", StringDeserializer.class);
        tmpMap.put("value.deserializer", StringDeserializer.class);
        tmpMap.put("group.id", GROUP_ID);
        tmpMap.put("auto.offset.reset", "latest");
        tmpMap.put("enable.auto.commit", "false");
        tmpMap.put("auto.commit.interval.ms", "60000"); // every interval update the kafka offset so that next read happens from that offset
        tmpMap.put("session.timeout.ms", "120000");
        tmpMap.put("request.timeout.ms", "120001");
        KAFKA_PARAMS = Collections.unmodifiableMap(tmpMap);
    }

    private KafkaConfig() {
    }
}
