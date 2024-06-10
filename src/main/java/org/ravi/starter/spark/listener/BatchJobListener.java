package org.ravi.starter.spark.listener;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.streaming.scheduler.BatchInfo;
import org.apache.spark.streaming.scheduler.OutputOperationInfo;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.ravi.starter.spark.util.BatchOffsetsInfo;
import scala.collection.Iterable;

/**
 * @author raviteja.kothapalli
 */
public class BatchJobListener implements StreamingListener {

    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
        BatchInfo batchInfo = batchCompleted.batchInfo();
        if (batchInfo.numRecords() > 0) {
            System.out.println("Batch Id: " + BatchOffsetsInfo.INSTANCE.getBatchId());
            System.out.println("Offset range: " + BatchOffsetsInfo.INSTANCE.getFromOffset() + ", " + BatchOffsetsInfo.INSTANCE.getToOffset());
            BatchOffsetsInfo.INSTANCE.reset();
        }
    }
}
