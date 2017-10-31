package com.zimbra.cs.contacts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.IntRange;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.ModelCache;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.graph.GatherNodesStream;
import org.apache.solr.client.solrj.io.graph.Traversal.Scatter;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.RankStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.UpdateParams;

import com.zimbra.common.account.ZAttrProvisioning.ContactAffinityIndexType;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.account.Server;
import com.zimbra.cs.index.LuceneFields;
import com.zimbra.cs.index.solr.AccountCollectionLocator;
import com.zimbra.cs.index.solr.JointCollectionLocator;
import com.zimbra.cs.index.solr.SolrCloudHelper;
import com.zimbra.cs.index.solr.SolrCollectionLocator;
import com.zimbra.cs.index.solr.SolrRequestHelper;
import com.zimbra.cs.index.solr.SolrUtils;

public class SolrContactGraph extends ContactGraph {

    private static final String FLD_NODE = "node";

    private static final String FLD_EDGE_ID = "id";
    private static final String FLD_FROM = "from";
    private static final String FLD_TO = "to";
    //dynamic fields
    private static final String FLD_EDGE_WEIGHT_FMT = "%s_edge_weight";
    private static final String FLD_EDGE_UPDATE_FMT = "%s_edge_update";

    private static final String COMBINED_EDGE_NAME = "combined";
    private static final String SKIP_EXISTING_DOCS_UPDATE_PROCESSOR = "skipexisting";

    private SolrCloudHelper solrHelper;

    public SolrContactGraph(SolrCloudHelper solrHelper, String accountId) {
        super(accountId);
        this.solrHelper = solrHelper;
    }

    private String getEdgeWeightField(String edgeName) {
        return String.format(FLD_EDGE_WEIGHT_FMT, edgeName.toLowerCase());
    }

    private String getEdgeUpdateField(String edgeName) {
        return String.format(FLD_EDGE_UPDATE_FMT, edgeName.toLowerCase());
    }

    private String getAccountFilter() {
        return new TermQuery(new Term(LuceneFields.L_ACCOUNT_ID, accountId)).toString();
    }

    TupleStream getInitialSearchStream(String zkHost, String collection, Collection<ContactNode> contacts) throws IOException {
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (ContactNode contact: contacts) {
            queryBuilder.add(getTermQuery(contact, FLD_FROM), Occur.SHOULD);
        }
        Query query = queryBuilder.build();

        Map<String, String> searchParams = new HashMap<>();
        searchParams.put("q", query.toString());
        if (solrHelper.needsAccountFilter()) {
            searchParams.put("fq", getAccountFilter());
        }
        searchParams.put("fl", FLD_FROM);
        searchParams.put("sort", FLD_FROM + " desc");
        searchParams.put("qt", "/export");

        return new CloudSolrStream(zkHost, collection, new MapSolrParams(searchParams));
    }

    private BooleanQuery.Builder newBooleanQueryBuilder() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        if (solrHelper.needsAccountFilter()) {
            builder.add(new TermQuery(new Term(LuceneFields.L_ACCOUNT_ID, accountId)), Occur.MUST);
        }
        return builder;
    }

    private void addEdgeFieldLowerBound(BooleanQuery.Builder builder, String field, String lowerBound) {
        BytesRef lower = new BytesRef(lowerBound);
        Query rangeQuery = new TermRangeQuery(field, lower, null, true, true);
        builder.add(rangeQuery, Occur.MUST);
    }

    private String parseNodeName(String nodeName) {
        String[] parts = nodeName.split(":", 2);
        if (parts.length == 1 || parts[0].length() == 0) {
            return nodeName;
        } else {
            return parts[1];
        }
    }

    TupleStream getNodesStream(String zkHost, String collection, TupleStream stream, String edgeField, String edgeUpdateField, int lowerBound, Long updateCutoff) throws IOException {

        BooleanQuery.Builder builder = newBooleanQueryBuilder();
        addEdgeFieldLowerBound(builder, edgeField, String.valueOf(lowerBound));
        if (updateCutoff != null) {
            addEdgeFieldLowerBound(builder, edgeUpdateField, String.valueOf(updateCutoff));
        }
        Map<String, String> params = new HashMap<>();
        params.put("fq", builder.build().toString());

        //don't return root nodes
        Set<Scatter> scatter = new HashSet<>();
        scatter.add(Scatter.LEAVES);

        //calculate the sum of incoming edge weights at each destination node
        List<Metric> metrics = new ArrayList<>();
        metrics.add(new SumMetric(edgeField));
        return new GatherNodesStream(zkHost, collection, stream, FLD_FROM, FLD_FROM, FLD_TO, params, metrics, false, scatter, -1);

    }

    TupleStream getRankStream(TupleStream stream, String edgeField, int numResults) throws IOException {
        StreamComparator comparator = new FieldComparator(String.format("sum(%s)", edgeField), ComparatorOrder.DESCENDING);
        return new RankStream(stream, numResults, comparator);
    }

    TupleStream getStreamingQuery(String zkHost, String collection, String edgeField, String edgeUpdateField, int lowerBound,
            Long updateCutoff, int numResults, Collection<ContactNode> contacts) throws IOException {

        /*
         * First we search for all starting nodes in the contact graph
         */
        TupleStream searchStream = getInitialSearchStream(zkHost, collection, contacts);

        /*
         * Next, we expand the frontier by 1 level, filtering out old edges and edges with the wrong type
         */
        TupleStream nodesStream = getNodesStream(zkHost, collection, searchStream, edgeField, edgeUpdateField, lowerBound, updateCutoff);

        /*
         * Finally, select the top N results
         */
        TupleStream rankStream = getRankStream(nodesStream, edgeField, numResults);
        return rankStream;
    }

    @Override
    public List<ContactResult> getRelatedContacts(ContactsParams params) throws ServiceException {
        Collection<ContactNode> contacts = params.getInputContacts();
        int numResults = params.getNumResults();
        int lowerBound = params.getLowerBound();
        Long cutoff = params.hasUpdateCutoff() ? params.getUpdateCutoff() : null;

        List<ContactResult> results = new ArrayList<>();

        String zkHost = solrHelper.getZkHost();
        String collection = solrHelper.getCoreName(accountId);

        String edgeName = params.isAllEdges() ? COMBINED_EDGE_NAME : params.getEdgeType().getName();
        try(TupleStream stream = getStreamingQuery(zkHost, collection, getEdgeWeightField(edgeName),
                getEdgeUpdateField(edgeName), lowerBound, cutoff, numResults, contacts)) {
            String weightField = String.format("sum(%s)", getEdgeWeightField(edgeName));
            StreamContext context = new StreamContext();
            SolrClientCache cache = new SolrClientCache();
            context.setSolrClientCache(cache);
            context.setModelCache(new ModelCache(100, zkHost, cache));
            stream.setStreamContext(context);
            stream.open();
            while(true) {
                Tuple tuple = stream.read();
                if (tuple.EOF) {
                    return results;
                }
                String contact = parseNodeName(tuple.getString(FLD_NODE));
                double weight = tuple.getDouble(weightField);
                results.add(new ContactResult(contact, weight));
            }
        } catch (IOException e) {
            throw ServiceException.FAILURE("unable to generate streaming query", e);
        }
    }

    private String generateDocId(ContactNode contact1, ContactNode contact2) {
        String edgeId = String.format("%s-%s", getNodeName(contact1), getNodeName(contact2));
        if (solrHelper.needsAccountFilter()) {
            return String.format("%s:%s", accountId, edgeId);
        } else {
            return edgeId;
        }
    }

    private String getNodeName(ContactNode contactNode) {
        if (contactNode.hasDataSourceId()) {
            return String.format("%s:%s", contactNode.getDataSourceId(), contactNode.getEmail());
        } else {
            return contactNode.getEmail();
        }
    }

    private Query getTermQuery(ContactNode contactNode, String field) {
        return new TermQuery(new Term(field, getNodeName(contactNode)));
    }

    @Override
    public void updateEdge(ContactNode contact1, ContactNode contact2, EdgeUpdate edgeUpdate) throws ServiceException {
        int multiplier = edgeUpdate.getMultiplier();
        EdgeType edgeType = edgeUpdate.getType();
        long timestamp = edgeUpdate.getTimestamp();
        boolean directed = edgeUpdate.isDirected();
        if (multiplier < 1) {
            throw ServiceException.INVALID_REQUEST("edge weight multiplier cannot be < 1", null);
        }
        UpdateRequest request = new UpdateRequest();
        request.setParam(CoreAdminParams.COLLECTION, solrHelper.getCoreName(accountId));
        request.setParam(UpdateParams.UPDATE_CHAIN, SKIP_EXISTING_DOCS_UPDATE_PROCESSOR);

        int increment = multiplier * edgeType.getValue();
        //Create an initial edge document with zero weights.
        //If this already exists, it will be skipped by the SkipExistingDocumentsProcessor.
        request.add(createEdgeDoc(contact1, contact2));
        if (!directed) {
            request.add(createEdgeDoc(contact2, contact1));
        }

        //increment the edge weight and timestamp
        request.add(createUpdateEdgeDoc(contact1, contact2, edgeType, timestamp, increment));
        if (!directed) {
            request.add(createUpdateEdgeDoc(contact2, contact1, edgeType, timestamp, increment));
        }

        solrHelper.execute(accountId, request);
    }

    private SolrInputDocument createUpdateEdgeDoc(ContactNode from, ContactNode to, EdgeType edgeType, long timestamp, int increment) {

        String edgeWeightField = getEdgeWeightField(edgeType.getName());
        String edgeUpdateField = getEdgeUpdateField(edgeType.getName());
        SolrInputDocument updateDoc = new SolrInputDocument();
        updateDoc.addField(FLD_EDGE_ID, generateDocId(from, to));

        //increment this EdgeType's edge weight
        incrementFieldValue(updateDoc, edgeWeightField, increment);

        //update timestamp
        setTimestampValue(updateDoc, edgeUpdateField, timestamp);

        //update edge representing sum of all edges
        incrementFieldValue(updateDoc, getEdgeWeightField(COMBINED_EDGE_NAME), increment);
        setTimestampValue(updateDoc, getEdgeUpdateField(COMBINED_EDGE_NAME), timestamp);

        return updateDoc;
    }

    private void setTimestampValue(SolrInputDocument solrDoc, String fieldName, long timestamp) {
        updateFieldValue(solrDoc, fieldName, String.valueOf(timestamp), "set");
    }

    private void incrementFieldValue(SolrInputDocument solrDoc, String fieldName, int incValue) {
        updateFieldValue(solrDoc, fieldName, String.valueOf(incValue), "inc");
    }

    private void updateFieldValue(SolrInputDocument solrDoc, String fieldName, String value, String updateOp) {
        Map<String, String> updateMap = new HashMap<>();
        updateMap.put(updateOp, value);
        solrDoc.addField(fieldName, updateMap);
    }

    private SolrInputDocument createEdgeDoc(ContactNode from, ContactNode to) {
        SolrInputDocument edgeDoc = new SolrInputDocument();
        edgeDoc.addField(FLD_EDGE_ID, generateDocId(from, to));
        edgeDoc.addField(FLD_FROM, from.getEmail());
        edgeDoc.addField(FLD_TO, to.getEmail());
        return edgeDoc;
    }

    @Override
    public void deleteNode(ContactNode contact) throws ServiceException {
        UpdateRequest req = solrHelper.newRequest(accountId);
        BooleanQuery.Builder builder = newBooleanQueryBuilder();
        BooleanQuery.Builder edgeQueryBuilder = new BooleanQuery.Builder();
        edgeQueryBuilder.add(getTermQuery(contact, FLD_FROM), Occur.SHOULD);
        edgeQueryBuilder.add(getTermQuery(contact, FLD_TO), Occur.SHOULD);
        builder.add(edgeQueryBuilder.build(), Occur.MUST);
        req.deleteByQuery(builder.build().toString());
        solrHelper.execute(accountId, req);
    }

    @Override
    public void deleteGraph() throws ServiceException {
        UpdateRequest req = solrHelper.newRequest(accountId);
        BooleanQuery.Builder builder = newBooleanQueryBuilder();
        BooleanQuery.Builder edgeQueryBuilder = new BooleanQuery.Builder();
        builder.add(edgeQueryBuilder.build(), Occur.MUST);
        edgeQueryBuilder.add(new TermQuery(new Term(FLD_FROM, "*")), Occur.SHOULD);
        edgeQueryBuilder.add(new TermQuery(new Term(FLD_TO, "*")), Occur.SHOULD);
        builder.add(edgeQueryBuilder.build(), Occur.MUST);
        req.deleteByQuery(builder.build().toString());
        solrHelper.execute(accountId, req);
    }

    @Override
    public void pruneEdges(long updateCutoff) throws ServiceException {
        UpdateRequest req = solrHelper.newRequest(accountId);
        BooleanQuery.Builder builder = newBooleanQueryBuilder();
        BytesRef upper = new BytesRef(String.valueOf(updateCutoff));
        TermRangeQuery range = new TermRangeQuery(getEdgeUpdateField(COMBINED_EDGE_NAME), null, upper, true, true);
        builder.add(range, Occur.MUST);
        req.deleteByQuery(builder.build().toString());
        solrHelper.execute(accountId, req);
    }

    @Override
    public void deleteEdge(ContactNode contact1, ContactNode contact2)
            throws ServiceException {
        UpdateRequest req = solrHelper.newRequest(accountId);
        BooleanQuery.Builder builder = newBooleanQueryBuilder();
        BooleanQuery.Builder edgeQueryBuilder = new BooleanQuery.Builder();
        edgeQueryBuilder.add(getTermQuery(contact1, FLD_FROM), Occur.SHOULD);
        edgeQueryBuilder.add(getTermQuery(contact2, FLD_TO), Occur.SHOULD);
        builder.add(edgeQueryBuilder.build(), Occur.MUST);
        req.deleteByQuery(builder.build().toString());
        solrHelper.execute(accountId, req);
    }


    @Override
    public void deleteDataSource(String dataSourceId) throws ServiceException {
        UpdateRequest req = solrHelper.newRequest(accountId);
        BooleanQuery.Builder builder = newBooleanQueryBuilder();
        BooleanQuery.Builder dsQueryBuilder = new BooleanQuery.Builder();
        dsQueryBuilder.add(new PrefixQuery(new Term(FLD_FROM, dataSourceId)), Occur.SHOULD);
        dsQueryBuilder.add(new PrefixQuery(new Term(FLD_TO, dataSourceId)), Occur.SHOULD);
        builder.add(dsQueryBuilder.build(), Occur.MUST);
        req.deleteByQuery(builder.build().toString());
        solrHelper.execute(accountId, req);
    }

    public static class Factory implements ContactGraph.Factory {

        private CloudSolrClient client;
        private SolrCloudHelper solrHelper;
        private static String COLLECTION_NAME_OR_PREFIX = "contacts";
        private static final String CONFIGSET = "events";

        public Factory() {
            try {
                Server server = Provisioning.getInstance().getLocalServer();
                String zkUrl = server.getContactAffinityBackendURL().substring("solrcloud:".length());
                client = SolrUtils.getCloudSolrClient(zkUrl);
                SolrCollectionLocator coreLocator;
                switch(server.getContactAffinityIndexType()) {
                case combined:
                    coreLocator = new JointCollectionLocator(COLLECTION_NAME_OR_PREFIX);
                    break;
                case account:
                default:
                    coreLocator = new AccountCollectionLocator(COLLECTION_NAME_OR_PREFIX);
                }
                solrHelper = new SolrCloudHelper(coreLocator, client, CONFIGSET);
            } catch (ServiceException e) {
                ZimbraLog.index.error("unable to instantiate SolrContactGraph Factory", e);
            }
        }

        @Override
        public ContactGraph getContactGraph(String accountId) throws ServiceException {
            if (solrHelper == null) {
                throw ServiceException.FAILURE("SolrContactGraph Factory not configured", null);
            }
            return new SolrContactGraph(solrHelper, accountId);
        }

        @Override
        public void shutdown() {
            try {
                client.close();
            } catch (IOException e) {
                ZimbraLog.index.error("error closing contact affinity CloudSolrClient", e);
            }
        }
    }
}
