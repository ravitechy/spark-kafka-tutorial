package org.ravi.starter.spark.event;

import java.io.Serializable;

/**
 * @author raviteja.kothapalli
 */
public class NormalizedEvent implements Serializable {

    private String normalizer;
    private String rawEvent;

    public NormalizedEvent(String normalizer, String rawEvent) {
        this.normalizer = normalizer;
        this.rawEvent = rawEvent;
    }

    public String getNormalizer() {
        return normalizer;
    }

    public String getRawEvent() {
        return rawEvent;
    }

    @Override
    public String toString() {
        return "NormalizedEvent{" +
                "normalizer='" + normalizer + '\'' +
                ", rawEvent='" + rawEvent + '\'' +
                '}';
    }
}
