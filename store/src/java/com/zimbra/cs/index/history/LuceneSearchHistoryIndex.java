package com.zimbra.cs.index.history;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Account;
import com.zimbra.cs.index.IndexDocument;
import com.zimbra.cs.index.IndexStore;
import com.zimbra.cs.index.Indexer;
import com.zimbra.cs.index.LuceneFields;
import com.zimbra.cs.index.ZimbraIndexSearcher;
import com.zimbra.cs.index.ZimbraScoreDoc;
import com.zimbra.cs.index.ZimbraTopDocs;
import com.zimbra.cs.index.ZimbraTopFieldDocs;
import com.zimbra.cs.index.history.SearchHistoryStore.HistoryIndex;
import com.zimbra.cs.mailbox.Mailbox;

public class LuceneSearchHistoryIndex implements HistoryIndex{

    private IndexStore index;
    private Account acct;

    public LuceneSearchHistoryIndex(Mailbox mbox) throws ServiceException {
        index = IndexStore.getFactory().getIndexStore(mbox);
        acct = mbox.getAccount();
    }

    @Override
    public void add(int id, String searchString, long timestamp) throws ServiceException {
        IndexDocument doc = IndexDocument.fromSearchString(id, searchString, timestamp);
        try {
            Indexer indexer = index.openIndexer();
            indexer.addDocument(doc);
            indexer.close();
        } catch (IOException e) {
            ZimbraLog.search.error("unable to index search history entry %s", searchString, e);
        }
    }

    @Override
    public Collection<Integer> search(String searchString)
            throws ServiceException {
        Query query = new PrefixQuery(new Term(LuceneFields.L_SEARCH, searchString));
        Sort sort = new Sort(new SortField(LuceneFields.L_SORT_DATE, SortField.STRING, true));
        int numResults = acct.getSearchHistorySuggestLimit();
        try {
            ZimbraIndexSearcher searcher = index.openSearcher();
            ZimbraTopFieldDocs docs = searcher.search(query, null, numResults, sort);
            List<Integer> entryIds = new ArrayList<Integer>(docs.getTotalHits());
            for (ZimbraScoreDoc scoreDoc: docs.getScoreDocs()) {
                Document doc =  searcher.doc(scoreDoc.getDocumentID());
                entryIds.add(Integer.parseInt(doc.get(LuceneFields.L_SEARCH_ID)));
            }
            return entryIds;
        } catch (IOException e) {
            ZimbraLog.search.error("unable to search search history for prefix '%s'", searchString, e);
            return Collections.emptyList();
        }
    }

    @Override
    public void delete(Collection<Integer> ids) throws ServiceException {
        List<Integer> idList = new ArrayList<Integer>(ids);
        try {
            Indexer indexer = index.openIndexer();
            indexer.deleteDocument(idList, LuceneFields.L_SEARCH_ID);
            indexer.close();
        } catch (IOException e) {
            ZimbraLog.search.error("unable to delete %s search history docs from the index", ids.size(), e);
        }
    }

    @Override
    public void deleteAll() throws ServiceException {
        Term term = new Term(LuceneFields.L_ITEM_TYPE, IndexDocument.SEARCH_HISTORY_TYPE);
        Query query = new TermQuery(term);
        ZimbraIndexSearcher searcher;
        try {
            searcher = index.openSearcher();
            int historySize = searcher.docFreq(term);
            if (historySize == 0) {
                return; //nothing to do
            }
            ZimbraTopDocs docs = searcher.search(query, historySize);
            List<Integer> entryIds = new ArrayList<Integer>(docs.getTotalHits());
            for (ZimbraScoreDoc scoreDoc: docs.getScoreDocs()) {
                Document doc =  searcher.doc(scoreDoc.getDocumentID());
                entryIds.add(Integer.parseInt(doc.get(LuceneFields.L_SEARCH_ID)));
            }
            delete(entryIds);
        } catch (IOException e) {
            ZimbraLog.search.error("unable to delete search history", e);
        }
    }
}
