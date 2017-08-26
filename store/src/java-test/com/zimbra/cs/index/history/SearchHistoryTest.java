package com.zimbra.cs.index.history;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Account;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.index.history.InMemorySearchHistoryIndex;
import com.zimbra.cs.index.history.SearchHistoryStore.SearchHistoryMetadataParams;
import com.zimbra.cs.index.history.SearchHistoryStore.SearchHistoryParams;
import com.zimbra.cs.mailbox.Mailbox;
import com.zimbra.cs.mailbox.MailboxManager;
import com.zimbra.cs.mailbox.MailboxTestUtil;

public class SearchHistoryTest {

    private static Provisioning prov;
    private static Account acct;
    private static Mailbox mbox;
    private static SearchHistoryStore store;

    @BeforeClass
    public static void init() throws Exception {
        MailboxTestUtil.initServer();
        prov = Provisioning.getInstance();
        acct = prov.createAccount("searchHistoryTest@zimbra.com", "test123", new HashMap<String, Object>());
        mbox = MailboxManager.getInstance().getMailboxByAccount(acct);
        store = SearchHistoryStore.getInstance();
    }

    @Before
    public void setUp() throws Exception {
        long timestamp = System.currentTimeMillis() - 4000;
        store.add(mbox, "search1", timestamp);
        store.add(mbox, "search2", timestamp + 1000);
        store.add(mbox, "search3", timestamp + 2000);
        store.add(mbox, "search1", timestamp + 3000);
        store.add(mbox, "another", timestamp + 4000);
    }

    @After
    public void tearDown() throws Exception {
        store.deleteHistory(mbox);
        prov.deleteAccount(acct.getId());
    }

    @Test
    public void testInMemorySearchHistoryIndex() throws Exception {
        InMemorySearchHistoryIndex index = new InMemorySearchHistoryIndex();
        long timestamp = System.currentTimeMillis();
        index.add(1, "search1", timestamp);
        index.add(2, "search2", timestamp);
        index.add(3, "foo1", timestamp);
        index.add(4, "foo2", timestamp);

        String[] prefixes = new String[] {
                "s", "se", "sea", "sear", "searc", "search",
                "search1", "search2",
                "f", "fo", "foo",
                "foo1", "foo2"};

        assertEquals("prefix map should contain all prefixes", prefixes.length, index.prefixMap.keySet().size());
        for (String prefix: prefixes) {
            assertTrue("prefix " + prefix + " should be in the map", index.prefixMap.containsKey(prefix));
        }

        for (int i = 1; i <=4; i++ ) {
            assertTrue(String.format("id %d should be in the id map", i), index.idMap.containsKey(i));
        }
        Collection<Integer> ids = index.search("s");
        assertEquals("index should return 2 ids", 2, ids.size());
        assertTrue("id 1 should be in the result set", ids.contains(1));
        assertTrue("id 2 should be in the result set", ids.contains(2));

        ids = index.search("search");
        assertEquals("index should return 2 ids", 2, ids.size());
        assertTrue("id 1 should be in the result set", ids.contains(1));
        assertTrue("id 2 should be in the result set", ids.contains(2));

        ids = index.search("f");
        assertEquals("index should return 2 ids", 2, ids.size());
        assertTrue("id 3 should be in the result set", ids.contains(3));
        assertTrue("id 4 should be in the result set", ids.contains(4));

        List<Integer> idsToDelete = new ArrayList<Integer>();
        idsToDelete.add(2);
        idsToDelete.add(4);
        index.delete(idsToDelete);

        prefixes = new String[] {
                "s", "se", "sea", "sear", "searc", "search",
                "search1",
                "f", "fo", "foo",
                "foo1"};

        assertEquals("prefix map should contain all prefixes", prefixes.length, index.prefixMap.keySet().size());
        for (String prefix: prefixes) {
            assertTrue("prefix " + prefix + " should be in the map", index.prefixMap.containsKey(prefix));
        }

        assertTrue("id 1 should be in the id map", index.idMap.containsKey(1));
        assertTrue("id 3 should be in the id map", index.idMap.containsKey(3));
        assertFalse("id 2 should not be in the id map", index.idMap.containsKey(2));
        assertFalse("id 4 should not be in the id map", index.idMap.containsKey(4));

        ids = index.search("s");
        assertEquals("index should return 1 id", 1, ids.size());
        assertTrue("id 1 should be in the result set", ids.contains(1));

        ids = index.search("search");
        assertEquals("index should return 1 id", 1, ids.size());
        assertTrue("id 1 should be in the result set", ids.contains(1));

        ids = index.search("f");
        assertEquals("index should return 1 id", 1, ids.size());
        assertTrue("id 3 should be in the result set", ids.contains(3));

        index.deleteAll();
        assertTrue("prefix map should be empty", index.prefixMap.isEmpty());
        assertTrue("id map should be empty", index.idMap.isEmpty());
    }

    @Test
    public void testInMemorySearchHistoryMetadata() throws Exception {
        InMemorySearchHistoryMetadata md = new InMemorySearchHistoryMetadata();
        long timestamp1 = System.currentTimeMillis() - 3000;
        long timestamp2 = timestamp1 + 1000;
        long timestamp3 = timestamp1 + 2000;
        long timestamp4 = timestamp1 + 3000;

        //this search order will result in search1 -> search3 -> search2
        int id1 = md.add("search1", timestamp1);
        int id2 = md.add("search2", timestamp2);
        int id3 = md.add("search3", timestamp3);
        md.update("search1", timestamp4);

        assertTrue("search1 should exist in the metadata store", md.exists("search1"));
        assertTrue("search2 should exist in the metadata store", md.exists("search2"));
        assertTrue("search3 should exist in the metadata store", md.exists("search3"));

        assertEquals("search1 count should be 2", 2, md.getCount("search1", 0));
        assertEquals("search2 count should be 1", 1, md.getCount("search2", 0));
        assertEquals("search3 count should be 1", 1, md.getCount("search3", 0));
        assertEquals("search1 count should be 1 within 1500ms", 1, md.getCount("search1", 1500));

        SearchHistoryMetadataParams params = new SearchHistoryMetadataParams();
        List<String> results = md.search(params);
        assertEquals("should see 3 results", 3, results.size());
        assertEquals("first result should be search1", "search1", results.get(0));
        assertEquals("second result should be search3", "search3", results.get(1));
        assertEquals("third result should be search2", "search2", results.get(2));

        params = new SearchHistoryMetadataParams(2);

        results = md.search(params);
        assertEquals("should see 2 results", 2, results.size());
        assertEquals("first result should be search1", "search1", results.get(0));
        assertEquals("second result should be search3", "search3", results.get(1));

        params = new SearchHistoryMetadataParams(1);

        results = md.search(params);
        assertEquals("should see 1 results", 1, results.size());
        assertEquals("first result should be search1", "search1", results.get(0));

        params = new SearchHistoryMetadataParams(10);
        params.setMaxAge(1500);
        results = md.search(params);
        assertEquals("should see 2 results", 2, results.size());
        assertEquals("first result should be search1", "search1", results.get(0));
        assertEquals("second result should be search3", "search3", results.get(1));

        params = new SearchHistoryMetadataParams(10);
        List<Integer> ids = new ArrayList<Integer>();
        ids.add(id2);
        ids.add(id3);
        params.setIds(ids);
        results = md.search(params);
        assertEquals("should see 2 results", 2, results.size());
        assertEquals("first result should be search3", "search3", results.get(0));
        assertEquals("second result should be search2", "search2", results.get(1));

        params.setMaxAge(1500);
        results = md.search(params);
        assertEquals("should see 1 result", 1, results.size());
        assertEquals("first result should be search3", "search3", results.get(0));

        params = new SearchHistoryMetadataParams(1);
        params.setIds(ids);
        results = md.search(params);
        assertEquals("should see 1 result", 1, results.size());
        assertEquals("first result should be search3", "search3", results.get(0));

        //prune the oldest occurrence of search1 by age
        Collection<Integer> deleted = md.deleteByAge(3000);
        assertEquals("no entries should be marked as deleted", 0, deleted.size());
        assertEquals("search1 count should be 1", 1, md.getCount("search1", 0));
        //should still show up in search
        params = new SearchHistoryMetadataParams(3);
        results = md.search(params);
        assertEquals("should see 3 results", 3, results.size());
        assertEquals("first result should be search1", "search1", results.get(0));
        assertEquals("second result should be search3", "search3", results.get(1));
        assertEquals("first result should be search1", "search2", results.get(2));


        //prune search2 by age
        deleted = md.deleteByAge(2000);
        assertEquals("one entry should be marked as deleted", 1, deleted.size());
        assertEquals("id2 should be deleted", (Integer) id2, deleted.iterator().next());
        assertEquals("search2 count should be 0", 0, md.getCount("search2", 0));
        //shouldn't show up in searches
        params = new SearchHistoryMetadataParams(3);
        results = md.search(params);
        assertEquals("should see 2 results after pruning search2", 2, results.size());
        assertEquals("first result should be search1", "search1", results.get(0));
        assertEquals("second result should be search3", "search3", results.get(1));

        //delete everything
        md.deleteAll();
        params = new SearchHistoryMetadataParams(3);
        results = md.search(params);
        assertTrue("should see 0 results after pruning all history", results.isEmpty());
    }

    @Test
    public void testSearchHistory() throws Exception {

        SearchHistoryParams params = new SearchHistoryParams();
        List<String> history = store.getHistory(mbox, params);

        //test counts of searches
        assertEquals("'search1' should have 2 occurrences", 2, store.getCount(mbox, "search1"));
        assertEquals("'another' should have 1 occurrences", 1, store.getCount(mbox, "another"));
        assertEquals("non-existent search should have 0 occurrences", 0, store.getCount(mbox, "blah"));

        //reduce maxAge to push one of the 'search1' instances out of the window
        InMemorySearchHistoryFactory.setMaxAge(2000);
        assertEquals("'search1' should have 1 occurence within the window", 1, store.getCount(mbox, "search1"));
        InMemorySearchHistoryFactory.setMaxAge(0);

        assertEquals("should see 4 results", 4, history.size());
        assertEquals("first result should be 'another'", "another", history.get(0));
        assertEquals("second result should be 'search1'", "search1", history.get(1));
        assertEquals("third result should be 'search3'", "search3", history.get(2));
        assertEquals("fourth result should be 'search2'", "search2", history.get(3));

        //test limiting by number of results
        params.setNumResults(2);
        history = store.getHistory(mbox, params);
        assertEquals("should see 2 results", 2, history.size());
        assertEquals("first result should be 'another'", "another", history.get(0));
        assertEquals("second result should be 'search1'", "search1", history.get(1));

        //test prefix search
        params.setPrefix("se");
        params.setNumResults(0);
        history = store.getHistory(mbox, params);
        assertEquals("should see 3 results", 3, history.size());
        assertEquals("first result should be 'search1'", "search1", history.get(0));
        assertEquals("second result should be 'search3'", "search3", history.get(1));
        assertEquals("third result should be 'search2'", "search2", history.get(2));

        //test prefix search with limiting number of results
        params.setNumResults(2);
        history = store.getHistory(mbox, params);
        assertEquals("should see 2 results", 2, history.size());
        assertEquals("first result should be 'search1'", "search1", history.get(0));
        assertEquals("second result should be 'search3'", "search3", history.get(1));

        //test prefix search with limiting by age.
        //this has to be done on the HistoryConfig level
        params.setNumResults(0);
        InMemorySearchHistoryFactory.setMaxAge(1500);
        history = store.getHistory(mbox, params);
        assertEquals("should see 1 result", 1, history.size());
        assertEquals("first result should be 'search1'", "search1", history.get(0));
        InMemorySearchHistoryFactory.setMaxAge(2500);
        history = store.getHistory(mbox, params);
        assertEquals("should see 2 result", 2, history.size());
        assertEquals("first result should be 'search1'", "search1", history.get(0));
        assertEquals("second result should be 'search3'", "search3", history.get(1));

        store.purgeHistory(mbox, 1500);
        history = store.getHistory(mbox, new SearchHistoryParams());
        assertEquals("should see 2 results after pruning by age", 2, history.size());
        assertEquals("first result should be 'another'", "another", history.get(0));
        assertEquals("second result should be 'search1'", "search1", history.get(1));
        assertEquals("'search1' should have 1 occurrence after pruning", 1, store.getCount(mbox, "search1"));
    }
}
