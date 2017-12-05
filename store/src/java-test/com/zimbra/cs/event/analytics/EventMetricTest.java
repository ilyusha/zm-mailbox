package com.zimbra.cs.event.analytics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.event.Event;
import com.zimbra.cs.event.Event.EventType;
import com.zimbra.cs.event.analytics.AccountEventMetrics.MetricKey;
import com.zimbra.cs.event.analytics.ContactFrequencyMetric.ContactFrequencyParams;
import com.zimbra.cs.event.analytics.EventMetric.MetricInitializer;
import com.zimbra.cs.event.analytics.EventMetric.MetricType;
import com.zimbra.cs.event.analytics.MetricData.RatioMetric;
import com.zimbra.cs.event.analytics.MetricData.ValueMetric;
import com.zimbra.cs.event.analytics.ReadRatioMetric.ReadRatioParams;
import com.zimbra.cs.event.logger.BatchingEventLogger;
import com.zimbra.cs.event.logger.EventMetricCallback;

public class EventMetricTest {

    @Test
    public void testValueMetric() throws Exception {

        MetricInitializer<ValueMetric, Integer> initializer = new MetricInitializer<ValueMetric, Integer>() {

            @Override
            public ValueMetric getInitialData() {
                return new ValueMetric(0);
            }
        };

        EventMetric<ValueMetric, Integer> metric = new EventMetric<ValueMetric, Integer>("testAccountID", null, initializer) {

            @Override
            public void increment(List<Event> events) throws ServiceException {
                this.metricData.increment(new ValueMetric(1));
            }
        };

        assertEquals("initial value should be 0", (Integer)0, metric.getValue());
        metric.increment(null);
        metric.increment(null);
        assertEquals("value after two increments should be 2", (Integer)2, metric.getValue());
    }

    @Test
    public void testRatioMetric() throws Exception {

        MetricInitializer<RatioMetric, Float> initializer = new MetricInitializer<RatioMetric, Float>() {

            @Override
            public RatioMetric getInitialData() {
                return new RatioMetric(0, 1);
            }
        };

        EventMetric<RatioMetric, Float> metric = new EventMetric<RatioMetric, Float>("testAccountID", null, initializer) {

            @Override
            public void increment(List<Event> events) throws ServiceException {
                this.metricData.increment(new RatioMetric(1, 2));
            }
        };

        assertEquals("initial value should be 0", new Float(0), metric.getValue());
        metric.increment(null);
        assertEquals("new value should be 1/3", new Float((float)1/3), metric.getValue());
        metric.increment(null);
        assertEquals("new value should be 2/5", new Float((float)2/5), metric.getValue());
    }

    private List<Event> getContactFrequencyEvents(String accountId, String contactEmail) {
        List<Event> events = new ArrayList<Event>();
        Event receivedEvent = Event.generateEvent(accountId, 1, contactEmail, "me@zimbra.com", EventType.RECEIVED, null, null, System.currentTimeMillis());
        Event sentEvent = Event.generateSentEvent(accountId, 2, "me@zimbra.com", contactEmail, null, null, System.currentTimeMillis());
        Event sentEventOtherRecip = Event.generateSentEvent(accountId, 3, "me@zimbra.com", "other@zimbra.com", null, null, System.currentTimeMillis());
        Event sentEventOtherAcct = Event.generateSentEvent("otherAccountId", 4, "me@zimbra.com", contactEmail, null, null, System.currentTimeMillis());
        events.add(receivedEvent);
        events.add(sentEvent);
        events.add(sentEventOtherRecip);
        events.add(sentEventOtherAcct);
        return events;
    }

    private List<Event> getReadRatioEvents(String accountId, String contactEmail) {
        List<Event> events = new ArrayList<Event>();
        events.add(Event.generateEvent(accountId, 1, contactEmail, "me@zimbra.com", EventType.SEEN, null, null, System.currentTimeMillis()));
        events.add(Event.generateEvent(accountId, 1, contactEmail, "me@zimbra.com", EventType.READ, null, null, System.currentTimeMillis()));
        events.add(Event.generateEvent(accountId, 2, contactEmail, "me@zimbra.com", EventType.SEEN, null, null, System.currentTimeMillis()));
        events.add(Event.generateEvent(accountId, 2, contactEmail, "me@zimbra.com", EventType.READ, null, null, System.currentTimeMillis()));
        events.add(Event.generateEvent(accountId, 3, contactEmail, "me@zimbra.com", EventType.SEEN, null, null, System.currentTimeMillis()));
        events.add(Event.generateEvent(accountId, 4, "other@zimbra.com", "me@zimbra.com", EventType.SEEN, null, null, System.currentTimeMillis()));
        return events;
    }

    @Test
    public void testContactFrequency() throws Exception {

        AtomicBoolean initialized = new AtomicBoolean(false);
        MetricInitializer<ValueMetric, Integer> initializer = new MetricInitializer<ValueMetric, Integer>() {

            @Override
            public ValueMetric getInitialData() {
                initialized.set(true);
                return new ValueMetric(0);
            }
        };

        String accountId = "accountId";
        String contact = "test@zimbra.com";

        BatchingEventLogger updateLogger = new BatchingEventLogger(10, 0, new EventMetricCallback());

        //retrieve the contact frequency EventMetric instance
        EventMetricManager.registerMetricFactory(MetricType.CONTACT_FREQUENCY, new ContactFrequencyMetric.Factory());
        AccountEventMetrics metrics = EventMetricManager.getInstance().getMetrics(accountId);
        ContactFrequencyParams params = new ContactFrequencyParams(contact);
        params.setInitializer(initializer);
        MetricKey key = new MetricKey(MetricType.CONTACT_FREQUENCY, params);
        EventMetric metric = metrics.getMetric(key);

        assertTrue("initializer.getInitialValue() should have been triggered", initialized.get() == true);
        assertEquals("initial value should be 0", 0, metric.getValue());

        //log some events that should update the EventMetric
        for (Event event: getContactFrequencyEvents(accountId, contact)) {
            updateLogger.log(event);
        }
        updateLogger.sendAllBatched();
        assertEquals("new value should be 2", 2, metric.getValue());
    }

    @Test
    public void testReadPercentage() throws Exception {

        AtomicBoolean initialized = new AtomicBoolean(false);
        MetricInitializer<RatioMetric, Float> initializer = new MetricInitializer<RatioMetric, Float>() {

            @Override
            public RatioMetric getInitialData() {
                initialized.set(true);
                return new RatioMetric(0, 0);
            }
        };

        String accountId = "accountId";
        String contact = "test@zimbra.com";

        BatchingEventLogger updateLogger = new BatchingEventLogger(10, 0, new EventMetricCallback());

        //retrieve the read EventMetric instance
        EventMetricManager.registerMetricFactory(MetricType.PERCENT_OPEN, new ReadRatioMetric.Factory());
        AccountEventMetrics metrics = EventMetricManager.getInstance().getMetrics(accountId);
        ReadRatioParams params = new ReadRatioParams(contact);
        params.setInitializer(initializer);
        MetricKey key = new MetricKey(MetricType.PERCENT_OPEN, params);
        EventMetric metric = metrics.getMetric(key);

        assertTrue("initializer.getInitialValue() should have been triggered", initialized.get() == true);
        assertEquals("initial value should be 0", new Float(0), metric.getValue());

        //log some events that should update the EventMetric
        for (Event event: getReadRatioEvents(accountId, contact)) {
            updateLogger.log(event);
        }
        updateLogger.sendAllBatched();
        assertEquals("new value should be 2/3", new Float((float)2/3), metric.getValue());
    }
}
