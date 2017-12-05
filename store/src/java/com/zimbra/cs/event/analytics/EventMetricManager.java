package com.zimbra.cs.event.analytics;

import java.util.HashMap;
import java.util.Map;

public class EventMetricManager {

    private static Map<EventMetric.MetricType, EventMetric.Factory> factories = new HashMap<>();
    private Map<String, AccountEventMetrics> accountMap = new HashMap<>();

    private static EventMetricManager instance = new EventMetricManager();

    public static EventMetricManager getInstance() {
        return instance;
    }

    public AccountEventMetrics getMetrics(String accountId) {
        //TODO: synchronize reads, use cache loader?
        AccountEventMetrics metricSet = accountMap.get(accountId);
        if (metricSet == null) {
            metricSet = new AccountEventMetrics(accountId, factories);
            accountMap.put(accountId, metricSet);
        }
        return metricSet;
    }

    public static void registerMetricFactory(EventMetric.MetricType type, EventMetric.Factory factory) {
        factories.put(type, factory);
    }
}
