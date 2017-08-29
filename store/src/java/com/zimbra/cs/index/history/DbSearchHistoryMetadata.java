package com.zimbra.cs.index.history;

import java.util.Collection;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.db.DbPool.DbConnection;
import com.zimbra.cs.db.DbPool;
import com.zimbra.cs.db.DbSearchHistory;
import com.zimbra.cs.index.history.SearchHistoryStore.SearchHistoryMetadataParams;
import com.zimbra.cs.mailbox.Mailbox;

public class DbSearchHistoryMetadata implements SearchHistoryStore.HistoryMetadataStore {

    private Mailbox mbox;
    private DbSearchHistory db;

    public DbSearchHistoryMetadata(Mailbox mbox) {
        this.mbox = mbox;
        this.db = new DbSearchHistory(mbox);
    }

    @Override
    public List<String> search(SearchHistoryMetadataParams params) throws ServiceException {
        DbConnection conn = mbox.getOperationConnection();
        return db.search(conn, params);
    }

    @Override
    public boolean exists(String searchString) throws ServiceException {
        DbConnection conn = mbox.getOperationConnection();
        return db.isRegistered(conn, searchString);
    }

    @Override
    public void addSearch(String searchString, long millis) throws ServiceException {
        DbConnection conn = mbox.getOperationConnection();
        db.logSearch(conn, searchString, millis);
    }

    @Override
    public Collection<Integer> deleteByAge(long maxAgeMillis) throws ServiceException {
        DbConnection conn = mbox.getOperationConnection();
        return db.delete(conn, maxAgeMillis);
    }

    @Override
    public void deleteAll() throws ServiceException {
        DbConnection conn = mbox.getOperationConnection();
        db.deleteAll(conn);
    }

    @Override
    public int getCount(String searchString, long maxAgeMillis) throws ServiceException {
        DbConnection conn = mbox.getOperationConnection();
        return db.getCount(conn, searchString, maxAgeMillis);
    }

    @Override
    public void init(int id, String searchString) throws ServiceException {
        DbConnection conn = mbox.getOperationConnection();
        db.createNewSearch(conn, id, searchString);
    }
}
