package com.zimbra.cs.event.analytics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.Pair;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.event.Event;
import com.zimbra.cs.event.analytics.EventMetric.MetricParams;
import com.zimbra.cs.event.analytics.EventMetric.MetricType;

/**
 * Class encapsulating all event metrics for an account
 */
public class AccountEventMetrics {

    private String accountId;
    private Map<EventMetric.MetricType, EventMetric.Factory> factories;
    private Map<MetricKey, EventMetric> metrics;

    public AccountEventMetrics(String accountId, Map<EventMetric.MetricType, EventMetric.Factory> factories) {
        this.accountId = accountId;
        this.factories = factories;
        this.metrics = new HashMap<>();
    }

    /**
     * Return the specified EventMetric instance. If not loaded,
     * the metric will be initialized first.
     */
    public EventMetric getMetric(MetricKey key) throws ServiceException {
        EventMetric metric = metrics.get(key);
        if (metric == null) {
            MetricType type = key.getType();
            EventMetric.Factory factory = factories.get(type);
            if (factory == null) {
                throw ServiceException.FAILURE(String.format("no EventMetric factory found for metric type %s",  type.name()), null);
            }
            metric = factory.buildMetric(accountId, key.getParams());
            metrics.put(key, metric);
        }
        return metric;
    }

    /**
     * Update all loaded EventMetric instances
     */
    public void incrementAll(List<Event> events) throws ServiceException {
        for (EventMetric metric: metrics.values()) {
            //TODO: change to debug
            ZimbraLog.event.info("updating metric %s with %d events", metric, events.size());
            metric.increment(events);
        }
    }

    /**
     * Cache key used for accessing an EventMetric instance.
     * If the EventMetric for the given type and parameters does not exist,
     * it is instantiated using the specified MetricParams.
     */
    static class MetricKey extends Pair<EventMetric.MetricType, EventMetric.MetricParams> {

        public MetricKey(MetricType type, MetricParams params) {
            super(type, params);
        }

        public MetricType getType() {
            return getFirst();
        }

        public MetricParams getParams() {
            return getSecond();
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("type", getType())
                    .add("params", getParams()).toString();
        }
    }
}