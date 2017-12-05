package com.zimbra.cs.event.analytics;

import java.util.List;

import com.google.common.base.Objects;
import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.event.Event;
import com.zimbra.cs.event.Event.EventContextField;
import com.zimbra.cs.event.Event.EventType;
import com.zimbra.cs.event.EventStore;
import com.zimbra.cs.event.analytics.MetricData.ValueMetric;
import com.zimbra.cs.mime.ParsedAddress;

public class ContactFrequencyMetric extends EventMetric<ValueMetric, Integer> {

    private String contactEmail;

    public ContactFrequencyMetric(String accountId, ContactFrequencyParams params) {
        super(accountId, MetricType.CONTACT_FREQUENCY, params.getInitializer());
        this.contactEmail = params.getContactEmail();
    }

    @Override
    public void increment(List<Event> events) throws ServiceException {
        int increment = (int) events.stream().filter(event -> eventAffectsContactFrequency(event)).count();
        metricData.increment(new ValueMetric(increment));
    }

    private boolean eventAffectsContactFrequency(Event event) {
        EventType type = event.getEventType();
        if (!event.getAccountId().equals(accountId) || (type != EventType.SENT && type != EventType.RECEIVED)) {
            return false;
        }
        String eventContact = type == EventType.SENT ? (String) event.getContextField(EventContextField.RECEIVER)
                : (String) event.getContextField(EventContextField.SENDER);
        return contactEmail.equalsIgnoreCase(new ParsedAddress(eventContact).emailPart);
    }

    private static class Initializer extends EventStoreInitializer<ValueMetric, Integer> {

        private ContactFrequencyParams params;

        public Initializer(EventStore eventStore, ContactFrequencyParams params) {
            super(eventStore);
            this.params = params;
        }

        @Override
        public ValueMetric getInitialData() {
            String contactEmail = params.getContactEmail();
            ContactFrequencyParams.TimeRange timeRange = params.getTimeRange();
            return new ValueMetric(0); //replace with appropriate EventStore call based on params
        }
    }

    public static class ContactFrequencyParams extends EventMetric.MetricParams<ValueMetric, Integer> {

        public static enum TimeRange {
            //used for ML feature generation
            LAST_DAY,
            LAST_WEEK,
            LAST_MONTH,
            FOREVER,
            //used for contact analytics
            LAST_6MONTHS,
            LAST_YEAR;
        }

        private String contactEmail;
        private TimeRange timeRange;


        public ContactFrequencyParams(String contactEmail) {
            this.contactEmail = contactEmail;
        }

        public String getContactEmail() {
            return contactEmail;
        }

        public TimeRange getTimeRange() {
            return timeRange;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("contact", contactEmail)
                    .add("timeRange", timeRange).toString();
        }
    }

    public static class Factory implements EventMetric.Factory<ValueMetric, Integer> {

        public String contactEmail;

        @Override
        public EventMetric<ValueMetric, Integer> buildMetric(String accountId, MetricParams<ValueMetric, Integer> params) throws ServiceException {
            ContactFrequencyParams cfParams = (ContactFrequencyParams) params;
            if (cfParams.getInitializer() == null) {
                EventStore eventStore = EventStore.getFactory().getEventStore(accountId);
                cfParams.setInitializer(new Initializer(eventStore, cfParams));
            }
            return new ContactFrequencyMetric(accountId, cfParams);
        }
    }
}
