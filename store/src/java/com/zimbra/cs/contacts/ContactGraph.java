package com.zimbra.cs.contacts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.mail.Address;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.Pair;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Account;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.mime.ParsedAddress;
import com.zimbra.cs.util.AccountUtil;

/**
 * Class representing the graph of contact affinities
 * @author iraykin
 *
 */
public abstract class ContactGraph {

    //relative edge weights affect the combined edge weight; heavier edges
    //will have more influence on affinity when querying for related contacts
    //without a specified edge type
    private static final int TO_EDGE_WEIGHT = 3;
    private static final int CC_EDGE_WEIGHT = 2;
    private static final int BCC_EDGE_WEIGHT = 1;

    private static final int DEFAULT_MAX_CLIQUE_SIZE = 10;

    protected static Factory factory;
    protected String accountId;

    public ContactGraph(String accountId) {
        this.accountId = accountId;
    }

    public static Factory getFactory() throws ServiceException {
        if (factory == null) {
            setFactory(SolrContactGraph.Factory.class);
        }
        return factory;
    }

    public static void clearFactory() {
        factory.shutdown();
        factory = null;
    }

    public static final void setFactory(Class<? extends Factory> factoryClass) throws ServiceException {
        String className = factoryClass.getName();
        ZimbraLog.search.info("setting ContactGraph.Factory class %s", className);
        try {
            factory = factoryClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw ServiceException.FAILURE(String.format("unable to initialize ContactGraph factory %s", className), e);
        }
    }

    /**
     * Get contacts that are adjacent to the set of contacts specified in ContactsParams, with the given constraints.
     * Results are ordered by relevance.
     */
    public abstract List<ContactResult> getRelatedContacts(ContactsParams params) throws ServiceException;

    /**
     * Increment the directed edge weight between two contact nodes. If the edge does not currently exist, it is created.
     */
    public abstract void updateEdge(ContactNode contact1, ContactNode contact2, EdgeUpdate edgeUpdate) throws ServiceException;

    /**
     *Delete a contact from the graph
     */
    public abstract void deleteNode(ContactNode contact) throws ServiceException;

    /**
     * Delete all edges connecting two contacts. This will NOT prevent new edges
     * from being added in the future.
     */
    public abstract void deleteEdge(ContactNode contact1, ContactNode contact2) throws ServiceException;

    /**
     * Delete all nodes corresponding to a datasource
     */
    public abstract void deleteDataSource(String dataSourceId) throws ServiceException;

    /**
     * Delete the entire contact graph
     */
    public abstract void deleteGraph() throws ServiceException;

    /**
     * Prune all connections for which no edge types have been updated since the given cutoff timestamp
     */
    public abstract void pruneEdges(long updateCutoff) throws ServiceException;

    /**
     * Increment edge weights between all pairs in the provided list of contact nodes.
     * That this results in 2 * (N choose 2) edges being created or updated.
     */
    public void addClique(List<ContactNode> contacts, EdgeUpdate edgeUpdate) throws ServiceException {
        for (int i=0; i<contacts.size()-1; i++) {
            for (int j=i+1; j<contacts.size(); j++) {
                ContactNode from = contacts.get(i);
                ContactNode to = contacts.get(j);
                updateEdge(from, to, edgeUpdate);
            }
        }
    }

    private boolean contactIsMe(String contact, Account me) {
        try {
            return AccountUtil.addressMatchesAccountOrSendAs(me, contact);
        } catch (ServiceException e) {
            //don't want to propagate exception to filter
            return false;
        }
    }

    private String extractEmailPart(Address address) {
        return new ParsedAddress(address.toString()).emailPart;
    }

    private List<ContactNode> emailsToNodes(List<String> emails, String dsId) {
        return emails.stream().map(email -> new ContactNode(email, dsId)).collect(Collectors.toList());
    }

    private int getMaxCliqueSize() {
        try {
            return Provisioning.getInstance().getLocalServer().getContactAffinityMaxCliqueSize();
        } catch (ServiceException e) {
            ZimbraLog.index.error("unable to get value of zimbraContactAffinityMaxCliqueSize, defaulting to %d", DEFAULT_MAX_CLIQUE_SIZE);
            return DEFAULT_MAX_CLIQUE_SIZE;
        }
    }
    private List<String> getContactsByType(MimeMessage mm, javax.mail.Message.RecipientType type, Account me) {
        try {
            Address[] recipients = mm.getRecipients(type);
            if (recipients == null) {
                return Collections.emptyList();
            }
            List<Address> addrs = Arrays.asList(recipients);
            Stream<String> contacts = addrs.stream().map(address->extractEmailPart(address));
            if (me != null) {
                contacts = contacts.filter(contact -> !contactIsMe(contact, me));
            }
            return contacts.collect(Collectors.toList());
        } catch (MessagingException e) {
            ZimbraLog.contact.error("unable to get contacts of type %s for contact affinity", type.toString(), e);
            return Collections.emptyList();
        }
    }

    public void updateFromSentMimeMessage(MimeMessage mm, long timestamp, String dsId) {
        /*
         * When sending a message, affinity is strengthened between every pair of contacts on the TO, CC, and BCC
         * recipient lists.
         */
        List<String> toContacts = getContactsByType(mm, javax.mail.internet.MimeMessage.RecipientType.TO, null);
        List<String> ccContacts = getContactsByType(mm, javax.mail.internet.MimeMessage.RecipientType.CC, null);
        List<String> bccContacts = getContactsByType(mm, javax.mail.internet.MimeMessage.RecipientType.BCC, null);
        updateCliqueFromContacts(emailsToNodes(toContacts, dsId), new EdgeUpdate(EdgeType.TO, timestamp));
        updateCliqueFromContacts(emailsToNodes(ccContacts, dsId), new EdgeUpdate(EdgeType.CC, timestamp));
        updateCliqueFromContacts(emailsToNodes(bccContacts, dsId), new EdgeUpdate(EdgeType.BCC, timestamp));
    }

    public void updateFromReceivedMimeMessage(MimeMessage mm, long timestamp, Account recipAcct, String dsId) {
        /*
         * When receiving a message, affinity is strengthened between every pair of recipients.
         * Additionally, the sender's one-directional affinity with every recipient is strengthened.
         *
         * For example, if the message is as follows:
         * FROM: john
         * TO: <this account>, bob, mark
         * CC: alice
         *
         * Then the following affinities will be updated:
         * john --TO--> bob
         * john --TO--> mark
         * john --CC--> alice
         * bob <--TO--> mark
         *
         * Notice that recipients are not linked to the sender.
         *
         */
        List<String> toContacts = getContactsByType(mm, javax.mail.internet.MimeMessage.RecipientType.TO, recipAcct);
        List<String> ccContacts = getContactsByType(mm, javax.mail.internet.MimeMessage.RecipientType.CC, recipAcct);
        updateCliqueFromContacts(emailsToNodes(toContacts, dsId), new EdgeUpdate(EdgeType.TO, timestamp));
        updateCliqueFromContacts(emailsToNodes(ccContacts, dsId), new EdgeUpdate(EdgeType.CC, timestamp));
        try {
            Address[] from = mm.getFrom();
            if (from == null) {
                return;
            }
            for (Address addr: from) {
                String fromEmail = extractEmailPart(addr);
                ContactNode senderNode = new ContactNode(fromEmail);
                updateSenderAffinity(emailsToNodes(toContacts, dsId), senderNode, new EdgeUpdate(EdgeType.TO, timestamp));
                updateSenderAffinity(emailsToNodes(ccContacts, dsId), senderNode, new EdgeUpdate(EdgeType.CC, timestamp));
            }

        } catch (MessagingException e) {
            ZimbraLog.contact.error("unable to get sender for contact affinity", e);
        }
    }

    private void updateSenderAffinity(List<ContactNode> contacts, ContactNode sender, EdgeUpdate edgeUpdate) {
        if (contacts.isEmpty()) {
            return;
        } else {
            for (ContactNode contact: contacts) {
                try {
                    updateEdge(sender, contact, edgeUpdate);
                } catch (ServiceException e) {
                    ZimbraLog.contact.error("unable to update sender contact affinity", e);
                }
            }
        }
    }

    private void updateCliqueFromContacts(List<ContactNode> contacts, EdgeUpdate edgeUpdate) {
        if (contacts.size() < 2) {
            return;
        }
        else if (contacts.size() > getMaxCliqueSize()) {
            ZimbraLog.contact.debug("not updating contact affinity due to excessive size of contact list");
        } else {

            try {
                addClique(contacts, edgeUpdate);
            } catch (ServiceException e) {
                ZimbraLog.contact.error("unable to update affinity of type '%s' for %d contacts", edgeUpdate.getType().getName(), contacts.size(), e);
            }
        }
    }

    public static class ContactNode {
        private String contactEmail;
        private String dataSourceId;

        public ContactNode(String contactEmail) {
            this(contactEmail, null);
        }

        public ContactNode(String contactEmail, String dataSourceId) {
            this.contactEmail = contactEmail;
            this.dataSourceId = dataSourceId;
        }

        public String getEmail() {
            return contactEmail;
        }

        public boolean hasDataSourceId() {
            return dataSourceId != null;
        }

        public String getDataSourceId() {
            return dataSourceId;
        }
    }

    public static class EdgeUpdate {
        private EdgeType type;
        private int multiplier;
        private long updateTimestamp;
        private boolean directed;

        public EdgeUpdate(EdgeType type, long timestamp) {
            this(type, timestamp, 1, false);
        }

        public EdgeUpdate(EdgeType type, long timestamp,
                int multipler, boolean directed) {
            this.type = type;
            this.updateTimestamp = timestamp;
            this.multiplier = multipler;
            this.directed = directed;
        }

        public EdgeType getType() { return type; }
        public long getTimestamp() { return updateTimestamp; }
        public boolean isDirected() { return directed; }
        public int getMultiplier() { return multiplier; }
    }

    public static class EdgeType {

        private static Map<String, EdgeType> knownEdgeTypes = new HashMap<>();
        public static EdgeType TO = new EdgeType("to", TO_EDGE_WEIGHT);
        public static EdgeType CC = new EdgeType("cc", CC_EDGE_WEIGHT);
        public static EdgeType BCC = new EdgeType("bcc", BCC_EDGE_WEIGHT);
        static {
            addKnownEdgeType(TO);
            addKnownEdgeType(CC);
            addKnownEdgeType(BCC);
        }
        private String edgeName;
        private int edgeValue;

        public static void addKnownEdgeType(EdgeType edgeType) {
            knownEdgeTypes.put(edgeType.getName(), edgeType);
        }

        public static EdgeType getKnownEdgeType(String edgeName) {
            return knownEdgeTypes.get(edgeName);
        }

        public EdgeType(String edgeName, int edgeValue) {
            this.edgeName = edgeName;
            this.edgeValue = edgeValue;
        }

        public String getName() {
            return edgeName;
        }

        public int getValue() {
            return edgeValue;
        }
    }

    public static interface Factory {
        public ContactGraph getContactGraph(String accountId);
        public void shutdown();
    }

    public static class ContactsParams {
        private EdgeType edgeType;
        private boolean allEdges = false;
        private Long updateCutoff = null;
        private int numResults = 0;
        private int lowerBound = 1;
        private List<String> inputContacts;

        public ContactsParams(Collection<String> contacts) {
            inputContacts = new ArrayList<String>();
            inputContacts.addAll(contacts);
        }

        public ContactsParams(String... contacts) {
            this(Arrays.asList(contacts));
        }

        public EdgeType getEdgeType() {
            return edgeType;
        }

        public boolean isAllEdges() {
            return allEdges;
        }

        public boolean hasUpdateCutoff() {
            return updateCutoff != null;
        }

        public long getUpdateCutoff() {
            return updateCutoff;
        }

        public int getNumResults() {
            return numResults;
        }

        public int getLowerBound() {
            return lowerBound;
        }

        public Collection<String> getInputContacts() {
            return inputContacts;
        }

        public ContactsParams setNumResults(int num) {
            this.numResults = num;
            return this;
        }

        public ContactsParams setUpdateCutoff(long cutoff) {
            this.updateCutoff = cutoff;
            return this;
        }

        public ContactsParams setEdgeType(EdgeType type) {
            this.edgeType = type;
            return this;
        }

        public ContactsParams setLowerBound(int bound) {
            this.lowerBound = bound;
            return this;
        }

        public ContactsParams setAllEdges() {
            this.allEdges = true;
            return this;
        }
    }

    public static class ContactResult extends Pair<String, Double> {

        public ContactResult(String contactName, Double weight) {
            super(contactName, weight);
        }

        public String getName() {
            return getFirst();
        }

       public double getWeight() {
           return getSecond();
       }

    }
}
