package com.zimbra.cs.event.analytics;

import java.util.List;

import com.google.common.base.Objects;
import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.event.Event;
import com.zimbra.cs.event.EventStore;

/**
 * Representation of a metric calculated from event data.
 * A metric can be be incrementally updated in-memory to avoid having to
 * query the event store every time.
 */
public abstract class EventMetric<T extends MetricData<S>, S> {

    public static enum MetricType {
        CONTACT_FREQUENCY,
        PERCENT_OPEN,
        TIME_TO_OPEN,
        TIME_TO_OPEN_GLOBAL,
        REPLY_RATE,
        REPLY_RATE_GLOBAL;
    }

    protected String accountId;
    private MetricInitializer<T, S> initializer;
    private boolean initialized = false;
    private MetricType type;
    protected T metricData;

    public EventMetric(String accountId, MetricType type, MetricInitializer<T, S> initializer) {
        this.accountId = accountId;
        this.type = type;
        this.initializer = initializer;
        init();
    }

    public void init() {
        if (!initialized) {
            this.metricData = initializer.getInitialData();
            initialized = true;
        }
    }

    /**
     * Increment the metric value based on one or more events.
     * It is up to the implementation to determine which events apply the metric
     * and filter appropriately
     */
    public abstract void increment(List<Event> events) throws ServiceException;

    /**
     * Get the value of this metric
     */
    public S getValue() {
        return metricData.getValue();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("metricType", type)
                .add("value", metricData.getValue()).toString();
    }

    /**
     * Helper interface used to initialize EventMetric
     */
    public static interface MetricInitializer<T extends MetricData<S>, S> {

        public T getInitialData();
    }

    /**
     * Abstract class used to initialize EventMetric values from an EventStore
     */
    public static abstract class EventStoreInitializer<T extends MetricData<S>, S> implements MetricInitializer<T, S> {

        private EventStore eventStore;

        public EventStoreInitializer(EventStore eventStore) {
            this.eventStore = eventStore;
        }

        protected EventStore getEventStore() {
            return eventStore;
        }
    }

    /**
     * Class defining parameters for an EventMetric.
     * The base class lets the caller specify a custom MetricInitializer;
     * subclasses can provide further arguments
     */
    public static abstract class MetricParams<T extends MetricData<S>, S> {
        private MetricInitializer<T, S> initializer;

        void setInitializer(MetricInitializer<T, S> initializer) {
            this.initializer = initializer;
        }

        MetricInitializer<T, S> getInitializer() {
            return initializer;
        }
    }

    /**
     * Factory interface for building EventMetric instances
     */
    public static interface Factory<T extends MetricData<S>, S> {

        /**
         * Return an EventMetric instance for the specified account ID with the given parameters
         */
        public abstract EventMetric<T, S> buildMetric(String accountId, MetricParams<T, S> params) throws ServiceException;
    }
}
