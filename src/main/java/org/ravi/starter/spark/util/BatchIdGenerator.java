package org.ravi.starter.spark.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author raviteja.kothapalli
 */
public class BatchIdGenerator {
    public static final BatchIdGenerator INSTANCE = new BatchIdGenerator();
    private final AtomicLong batchIdGen = new AtomicLong();

    private BatchIdGenerator() {
    }

    public long generateBatchId() {
        if (batchIdGen.get() == Long.MAX_VALUE) {
            batchIdGen.set(0);
        }
        return batchIdGen.getAndIncrement();
    }
}
