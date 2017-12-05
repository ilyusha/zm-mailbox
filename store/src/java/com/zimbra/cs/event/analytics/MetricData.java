package com.zimbra.cs.event.analytics;

import com.zimbra.common.util.Pair;
import com.zimbra.common.util.ZimbraLog;

/**
 * Representation of intermediate data necessary to compute an <link>EventMetric</link>.
 *
 */
public interface MetricData<T> {

    /**
     * return the value of the metric
     */
    public T getValue();

    public void increment(MetricData<T> other);

    /**
     * Metric that measures an integer value. Incrementing it
     *
     */
    static class ValueMetric implements MetricData<Integer> {
        private int val;

        public ValueMetric(int val) {
            this.val = val;
        }
        @Override
        public Integer getValue() {
            return val;
        }

        @Override
        public void increment(MetricData<Integer> other) {
            val+= other.getValue();
        }
    }

    static class RatioMetric extends Pair<Integer, Integer> implements MetricData<Float> {


        public RatioMetric(int numerator, int denominator) {
            super(numerator, denominator);
        }

        @Override
        public Float getValue() {
            if (getSecond() == 0) {
                return (float)0; //this should be sufficient; no need to introduce NaN handling for division by zero cases
            }
            return (float)getFirst() / getSecond();
        }

        @Override
        public void increment(MetricData<Float> other) {
            RatioMetric otherRatio = (RatioMetric) other;
            setFirst(getFirst() + otherRatio.getFirst());
            setSecond(getSecond() + otherRatio.getSecond());
        }
    }
}