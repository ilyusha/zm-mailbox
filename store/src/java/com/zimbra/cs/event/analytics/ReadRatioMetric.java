package com.zimbra.cs.event.analytics;

import java.util.List;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.event.Event;
import com.zimbra.cs.event.EventStore;
import com.zimbra.cs.event.Event.EventContextField;
import com.zimbra.cs.event.Event.EventType;
import com.zimbra.cs.event.analytics.MetricData.RatioMetric;
import com.zimbra.cs.mime.ParsedAddress;

public class ReadRatioMetric extends EventMetric<RatioMetric, Float> {

    private String contactEmail;

    public ReadRatioMetric(String accountId, ReadRatioParams params) {
        super(accountId, MetricType.PERCENT_OPEN, params.getInitializer());
        this.contactEmail = params.getContactEmail();
    }

    private boolean eventMatchesContactAndType(Event event, EventType type) {
        if (!event.getAccountId().equals(accountId) || event.getEventType() != type) {
            return false;
        }
        String eventContact = (String) event.getContextField(EventContextField.SENDER);
        return contactEmail.equalsIgnoreCase(new ParsedAddress(eventContact).emailPart);
    }

    private int getNumReadEvents(List<Event> events) {
        return (int) events.stream().filter(event -> eventMatchesContactAndType(event, EventType.READ)).count();
    }

    private int getNumSeenEvents(List<Event> events) {
        return (int) events.stream().filter(event -> eventMatchesContactAndType(event, EventType.SEEN)).count();
    }

    @Override
    public void increment(List<Event> events) throws ServiceException {
        int numeratorInc = getNumReadEvents(events);
        int denominatorInc = getNumSeenEvents(events);
        RatioMetric increment = new RatioMetric(numeratorInc, denominatorInc);
        metricData.increment(increment);
    }

    private static class Initializer extends EventStoreInitializer<RatioMetric, Float> {

        private ReadRatioParams params;

        public Initializer(EventStore eventStore, ReadRatioParams params) {
            super(eventStore);
            this.params = params;
        }

        @Override
        public RatioMetric getInitialData() {
            String contact = params.getContactEmail();
            return new RatioMetric(0, 0); //replace with appropriate EventStore call based on params
        }
    }

    static class ReadRatioParams extends EventMetric.MetricParams<RatioMetric, Float> {

        private String contactEmail;

        public ReadRatioParams(String contactEmail) {
            this.contactEmail = contactEmail;
        }

        public String getContactEmail() {
            return contactEmail;
        }
    }

    public static class Factory implements EventMetric.Factory<RatioMetric, Float> {

        public String contactEmail;

        @Override
        public EventMetric<RatioMetric, Float> buildMetric(String accountId, MetricParams<RatioMetric, Float> params) throws ServiceException {
            ReadRatioParams rrParams = (ReadRatioParams) params;
            if (rrParams.getInitializer() == null) {
                EventStore eventStore = EventStore.getFactory().getEventStore(accountId);
                rrParams.setInitializer(new Initializer(eventStore, rrParams));
            }
            return new ReadRatioMetric(accountId, rrParams);
        }
    }

}
