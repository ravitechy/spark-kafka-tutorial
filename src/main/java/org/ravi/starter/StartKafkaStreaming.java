package org.ravi.starter;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.ravi.starter.spark.KafkaStreamInitializer;
import org.ravi.starter.spark.handler.DataStreamHandlerFactory;
import org.ravi.starter.spark.listener.BatchJobListener;

/**
 * @author raviteja.kothapalli
 */
public class StartKafkaStreaming {

    public static void main(String[] args) throws InterruptedException {
        JavaDStream<String> dataStream = KafkaStreamInitializer.initDataStream();
        DataStreamHandlerFactory.INSTANCE.handler.handleStream(dataStream);
        JavaStreamingContext streamingContext = KafkaStreamInitializer.streamingContext;
        streamingContext.addStreamingListener(new BatchJobListener());
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
