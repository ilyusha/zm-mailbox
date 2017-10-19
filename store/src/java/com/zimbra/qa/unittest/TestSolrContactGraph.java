package com.zimbra.qa.unittest;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.params.CoreAdminParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.account.Account;
import com.zimbra.cs.contacts.ContactGraph;
import com.zimbra.cs.contacts.ContactGraph.ContactNode;
import com.zimbra.cs.contacts.ContactGraph.ContactResult;
import com.zimbra.cs.contacts.ContactGraph.ContactsParams;
import com.zimbra.cs.contacts.ContactGraph.EdgeType;
import com.zimbra.cs.contacts.ContactGraph.EdgeUpdate;
import com.zimbra.cs.contacts.SolrContactGraph;
import com.zimbra.cs.index.solr.JointCollectionLocator;
import com.zimbra.cs.index.solr.SolrCloudHelper;
import com.zimbra.cs.index.solr.SolrCollectionLocator;
import com.zimbra.cs.index.solr.SolrUtils;

public class TestSolrContactGraph {

    private static CloudSolrClient client;
    private static String zkHost = "172.16.153.131:9983"; //temp
    private static String collection = "contact_graph_test";
    private static String USER1 = "SolrContactGraphTest-1";
    private static String USER2 = "SolrContactGraphTest-2";
    private static String acctId1;
    private static String acctId2;
    private SolrContactGraph graph1;
    private SolrContactGraph graph2;

    @Before
    public void setUp() throws Exception {
        client = SolrUtils.getCloudSolrClient(zkHost);
        cleanUp();
        acctId1 = TestUtil.createAccount(USER1).getId();
        acctId2 = TestUtil.createAccount(USER2).getId();
        SolrCollectionLocator locator = new JointCollectionLocator(collection);
        SolrCloudHelper helper = new SolrCloudHelper(locator, client, "events");
        graph1 = new SolrContactGraph(helper, acctId1);
        graph2 = new SolrContactGraph(helper, acctId2);
        buildGraphs();
    }

    private void cleanUp() throws Exception {
        CollectionAdminRequest.Delete deleteCollectionRequest = CollectionAdminRequest.deleteCollection(collection);
        try {
            deleteCollectionRequest.process(client);
        } catch (RemoteSolrException | SolrServerException | IOException e) {
        }
        TestUtil.deleteAccountIfExists(USER1);
        TestUtil.deleteAccountIfExists(USER2);
    }

    @After
    public void tearDown() throws Exception {
        cleanUp();
    }

    public void addClique(ContactGraph graph, int numTimes, EdgeType type, long timestamp, String... contacts) throws Exception {
        for (int i=0; i<numTimes; i++) {
            List<ContactNode> nodes = Arrays.asList(contacts)
                    .stream()
                    .map(contact -> new ContactNode(contact))
                    .collect(Collectors.toList());
            EdgeUpdate edgeUpdate = new EdgeUpdate(type, timestamp);
            graph.addClique(nodes, edgeUpdate);
        }
    }

    private void buildGraphs() throws Exception {
        long timestamp1 = 2000;
        long timestamp2 = 1000;

        //[A,B,C] is a clique with [A,B] having a stronger weight
        //[A,D] is a connection
        //[D,E,F,G] is a stronger clique
        addClique(graph1, 5, EdgeType.TO, timestamp1, "A", "B");
        addClique(graph1, 5, EdgeType.TO, timestamp1, "A", "B", "C");
        addClique(graph1, 1, EdgeType.TO, timestamp1, "A", "D");
        addClique(graph1, 10, EdgeType.TO, timestamp1, "D", "E");
        addClique(graph1, 10, EdgeType.TO, timestamp1, "D", "E", "F");
        addClique(graph1, 11, EdgeType.TO, timestamp1, "D", "E", "F", "G");


        //The [X,Y,Z] clique isn't connected to A via TO edges
        addClique(graph1, 1, EdgeType.TO, timestamp1, "X", "Y", "Z");

        //the [X,Y,Z] clique is connected to A only through a CC edge; and the Z connection is older
        addClique(graph1, 5, EdgeType.CC, timestamp2, "A", "X", "Y", "Z");
        addClique(graph1, 5, EdgeType.CC, timestamp1, "A", "X", "Y");
        addClique(graph1, 5, EdgeType.CC, timestamp1, "A", "X");

        //different contact graph; shouldn't affect graph1
        addClique(graph2, 100, EdgeType.TO, timestamp1, "A", "OTHER");

        commit();
    }

    private void commit() throws Exception {
        UpdateRequest commitReq = new UpdateRequest();
        commitReq.setAction(ACTION.COMMIT, true, true);
        commitReq.setParam(CoreAdminParams.COLLECTION, collection);
        commitReq.process(client);
    }

    private void testResults(List<ContactResult> results, String[] expected) {
        List<String> contactNames = results.stream().map(ContactResult::getName).collect(Collectors.toList());
        assertEquals(String.format("should see %d results", expected.length), expected.length, results.size());
        for (int i=0; i<expected.length; i++) {
            assertEquals(String.format("result %d should be %s", i+1, expected[i]), expected[i], contactNames.get(i));
        }
    }

    @Test
    public void testSearch() throws Exception {

        ContactsParams params = new ContactGraph.ContactsParams("A").setNumResults(10).setEdgeType(EdgeType.TO);
        List<ContactResult> results = graph1.getRelatedContacts(params);
        testResults(results, new String[] {"B", "C", "D"});

        params = new ContactGraph.ContactsParams("A", "D").setNumResults(10).setEdgeType(EdgeType.TO);
        results = graph1.getRelatedContacts(params);
        testResults(results, new String[] {"E", "F", "G", "B", "C"});

        params = new ContactGraph.ContactsParams("A", "D").setNumResults(3).setEdgeType(EdgeType.TO);
        results = graph1.getRelatedContacts(params);
        testResults(results, new String[] {"E", "F", "G"});

        params = new ContactGraph.ContactsParams("A").setNumResults(10).setEdgeType(EdgeType.CC);
        results = graph1.getRelatedContacts(params);
        testResults(results, new String[] {"X", "Y", "Z"});

        params = new ContactGraph.ContactsParams("A").setNumResults(10).setEdgeType(EdgeType.CC).setUpdateCutoff(1500);
        results = graph1.getRelatedContacts(params);
        testResults(results, new String[] {"X", "Y"});
    }


    @Test
    public void testPruneOldEdges() throws Exception {
        //sanity check
        ContactsParams params = new ContactGraph.ContactsParams("A").setNumResults(10).setEdgeType(EdgeType.CC);
        List<ContactResult >results = graph1.getRelatedContacts(params);
        testResults(results, new String[] {"X", "Y", "Z"});

        graph1.pruneEdges(1500); //should delete the [A,Z] edge
        params = new ContactGraph.ContactsParams("A").setNumResults(10).setEdgeType(EdgeType.CC);
        results = graph1.getRelatedContacts(params);
        testResults(results, new String[] {"X", "Y"});
    }
}
