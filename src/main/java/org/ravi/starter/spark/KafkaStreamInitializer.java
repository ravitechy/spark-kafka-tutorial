package org.ravi.starter.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.ravi.starter.kafka.KafkaConfig;
import org.ravi.starter.spark.util.BatchIdGenerator;
import org.ravi.starter.spark.util.BatchOffsetsInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author raviteja.kothapalli
 */
public class KafkaStreamInitializer {
    public static final JavaStreamingContext streamingContext;

    static {
        final SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreamer");
        conf.set("spark.streaming.backpressure.enabled", "true");
        conf.set("spark.streaming.kafka.maxRatePerPartition", "5");
        conf.set("spark.streaming.backpressure.initialRate", "5");
        conf.set("spark.streaming.receiver.maxRate", "5");
        streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
        streamingContext.checkpoint("D:\\spark-base\\CHECKPOINT_DIR_1");
    }

    private KafkaStreamInitializer() {
    }

    public static JavaDStream<String> initDataStream() {
        TopicPartition partition = new TopicPartition(KafkaConfig.TOPIC, 0);
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Assign(Set.of(partition), KafkaConfig.KAFKA_PARAMS));
        printOffsetRange(stream);
        JavaDStream<String> modifiedStream = stampUniqueId(stream);
        return modifiedStream.persist(StorageLevel.MEMORY_ONLY());
    }

    private static JavaDStream<String> stampUniqueId(JavaInputDStream<ConsumerRecord<String, String>> stream) {
        return stream.map(record -> {
            String event = record.value();
            String debug = record.topic() + "::" + record.partition() + "::" + record.offset();
            String toInsert = ",\"uniqueId\":\"" + debug;
            int strLen = event.length();
            int substrLenToConsider = event.substring(strLen - 2).contains("\"") ? strLen - 2 : strLen - 1;
            event = event.substring(0, substrLenToConsider)
                    + "\"" + toInsert
                    + event.substring(substrLenToConsider);
            return event;
        });
    }

    private static void printOffsetRange(JavaDStream<ConsumerRecord<String, String>> dataStream) {
        dataStream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            if (offsetRanges.length > 0) {
                for (OffsetRange offsetRange : offsetRanges) {
                    long batchCount = offsetRange.untilOffset() - offsetRange.fromOffset();
                    if (batchCount > 0) {
                        long batchId = BatchIdGenerator.INSTANCE.generateBatchId();
                        BatchOffsetsInfo.INSTANCE.setBatchId(batchId);
                        BatchOffsetsInfo.INSTANCE.setFromOffset(offsetRange.fromOffset());
                        BatchOffsetsInfo.INSTANCE.setToOffset(offsetRange.untilOffset());
                        System.out.println("Batch count : " + batchCount + ", From offset: " + offsetRange.fromOffset() + ", Until offset: " + offsetRange.untilOffset());
                    }
                }
            }
        });
    }
}
