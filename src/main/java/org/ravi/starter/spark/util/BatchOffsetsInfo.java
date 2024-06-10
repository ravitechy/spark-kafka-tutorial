package org.ravi.starter.spark.util;

/**
 * @author raviteja.kothapalli
 */
public class BatchOffsetsInfo {
    public static BatchOffsetsInfo INSTANCE = new BatchOffsetsInfo();
    private long fromOffset;
    private long toOffset;

    public void setFromOffset(long fromOffset) {
        System.out.println(Thread.currentThread().getName() + ": Setting the fromOffset value: " + fromOffset);
        this.fromOffset = fromOffset;
    }

    public void setToOffset(long toOffset) {
        System.out.println(Thread.currentThread().getName() + ": Setting the toOffset value: " + toOffset);
        this.toOffset = toOffset;
    }

    public long getFromOffset() {
        return fromOffset;
    }

    public long getToOffset() {
        return toOffset;
    }

    public void reset() {
        System.out.println(Thread.currentThread().getName() + ": Resetting the fromOffSet and toOffset values");
        this.fromOffset = 0;
        this.toOffset = 0;
    }
}
