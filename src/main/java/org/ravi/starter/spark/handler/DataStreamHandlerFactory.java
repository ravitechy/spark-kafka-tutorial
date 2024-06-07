package org.ravi.starter.spark.handler;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author raviteja.kothapalli
 */
public class DataStreamHandlerFactory {
    public static final DataStreamHandlerFactory INSTANCE = new DataStreamHandlerFactory();
    public final IDataStreamHandler handler = new DataStreamHandler();

    private DataStreamHandlerFactory() {
    }
}
