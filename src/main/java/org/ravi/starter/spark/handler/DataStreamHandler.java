package org.ravi.starter.spark.handler;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.json.JSONObject;
import org.ravi.starter.spark.event.NormalizedEvent;
import org.ravi.starter.spark.util.SparkKafkaUtil;

import java.util.List;

/**
 * @author raviteja.kothapalli
 */
public class DataStreamHandler implements IDataStreamHandler {

    @Override
    public void handleStream(JavaDStream<String> dataStream) {
        JavaDStream<NormalizedEvent> normalizedEventsStream = getNormalizedEventsStream(dataStream);
        normalizedEventsStream.foreachRDD((rdd) -> {
            List<NormalizedEvent> list = rdd.collect();
            if (CollectionUtils.isNotEmpty(list)) {
                System.out.println(list.size());
            }
        });
    }

    private JavaDStream<NormalizedEvent> getNormalizedEventsStream(JavaDStream<String> dataStream) {
        JavaDStream<NormalizedEvent> normalizedEventsStream = dataStream.
                filter(StringUtils::isNotBlank).
                map(str -> new NormalizedEvent(DataStreamHandler.class.getSimpleName(), str));
        return normalizedEventsStream;
    }
}
