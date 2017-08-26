package com.zimbra.cs.index.history;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zimbra.cs.account.Account;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.index.IndexDocument;
import com.zimbra.cs.index.IndexStore;
import com.zimbra.cs.index.LuceneFields;
import com.zimbra.cs.index.ZimbraIndexSearcher;
import com.zimbra.cs.index.ZimbraTopDocs;
import com.zimbra.cs.mailbox.Mailbox;
import com.zimbra.cs.mailbox.MailboxManager;
import com.zimbra.cs.mailbox.MailboxTestUtil;

public class LuceneSearchHistoryIndexTest {

    private IndexStore idxStore;
    private LuceneSearchHistoryIndex index;
    private Provisioning prov;
    private Account acct;
    private Mailbox mbox;

    @Before
    public void setUp() throws Exception {
        MailboxTestUtil.initServer();
        prov = Provisioning.getInstance();
        acct = prov.createAccount("luceneSearchHistoryIndexTest@zimbra.com", "test123", new HashMap<String, Object>());
        mbox = MailboxManager.getInstance().getMailboxByAccount(acct);
        index = new LuceneSearchHistoryIndex(mbox);
        idxStore = IndexStore.getFactory().getIndexStore(mbox);
        index.add(1, "test1", System.currentTimeMillis() - 1000);
        index.add(2, "test2", System.currentTimeMillis());
    }

    @After
    public void tearDown() throws Exception {
        index.deleteAll();
        idxStore.deleteIndex();
        prov.deleteAccount(acct.getId());
    }

    @Test
    public void testAdd() throws Exception {
        //verify that the index has the expected terms
        ZimbraIndexSearcher searcher = idxStore.openSearcher();
        Query query = new TermQuery(new Term(LuceneFields.L_ITEM_TYPE, "sh"));
        ZimbraTopDocs results = searcher.search(query, 10);
        assertEquals("should see two terms in the index", 2, results.getTotalHits());
    }

    @Test
    public void testSearch() throws Exception {
        Collection<Integer> ids = index.search("t");
        assertEquals("should get two IDs from the index", 2, ids.size());
        assertTrue("id 1 should be in the result set", ids.contains(1));
        assertTrue("id 2 should be in the result set", ids.contains(2));
    }

    @Test
    public void testDelete() throws Exception {
        List<Integer> toDelete = new ArrayList<Integer>(1);
        toDelete.add(1);
        index.delete(toDelete);

        //verify test1 is no longer in the index
        ZimbraIndexSearcher searcher = idxStore.openSearcher();
        Query query = new TermQuery(new Term(LuceneFields.L_ITEM_TYPE, "sh"));
        ZimbraTopDocs results = searcher.search(query, 10);
        assertEquals("should see one term in the index", 1, results.getTotalHits());

        //verify search only returns test2
        Collection<Integer> ids = index.search("t");
        assertEquals("should get one ID from the index", 1, ids.size());
        assertTrue("id 2 should be in the result set", ids.contains(2));
    }

    @Test
    public void deleteAll() throws Exception {
        index.deleteAll();
        ZimbraIndexSearcher searcher = idxStore.openSearcher();
        Query query = new TermQuery(new Term(LuceneFields.L_ITEM_TYPE, IndexDocument.SEARCH_HISTORY_TYPE));
        ZimbraTopDocs results = searcher.search(query, 10);
        assertEquals("should not see any matching terms in the index", 0, results.getTotalHits());
        Collection<Integer> ids = index.search("t");
        assertTrue("should not get any results from the index", ids.isEmpty());
    }
}
