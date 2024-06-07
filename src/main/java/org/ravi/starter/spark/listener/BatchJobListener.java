package org.ravi.starter.spark.listener;

import org.apache.spark.streaming.scheduler.BatchInfo;
import org.apache.spark.streaming.scheduler.OutputOperationInfo;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import scala.collection.Iterable;

/**
 * @author raviteja.kothapalli
 */
public class BatchJobListener implements StreamingListener {

    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
        BatchInfo batchInfo = batchCompleted.batchInfo();
        if (batchInfo.numRecords() > 0) {
            Iterable<OutputOperationInfo> values = batchCompleted.batchInfo().outputOperationInfos().values();
        }
    }
}
