package org.ravi.starter.spark.handler;

import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * @author raviteja.kothapalli
 */
public interface IDataStreamHandler {

    public void handleStream(JavaDStream<String> dataStream);
}
