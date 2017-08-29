package com.zimbra.cs.index.history;

import java.util.HashMap;
import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.index.history.SearchHistoryStore.HistoryConfig;
import com.zimbra.cs.index.history.SearchHistoryStore.HistoryIndex;
import com.zimbra.cs.index.history.SearchHistoryStore.HistoryMetadataStore;
import com.zimbra.cs.index.history.SearchHistoryStore.SavedSearchPromptLog;
import com.zimbra.cs.mailbox.Mailbox;

public class ZimbraSearchHistoryFactory implements SearchHistoryStore.Factory {

    private Map<String, SavedSearchPromptLog> promptLogCache = new HashMap<String, SavedSearchPromptLog>();

    @Override
    public HistoryIndex getIndex(Mailbox mbox) throws ServiceException {
        return new LuceneSearchHistoryIndex(mbox);
    }

    @Override
    public HistoryMetadataStore getMetadataStore(Mailbox mbox) {
        return new DbSearchHistoryMetadata(mbox);
    }

    @Override
    public HistoryConfig getConfig(Mailbox mbox) throws ServiceException {
        return new LdapSearchHistoryConfig(mbox.getAccount());
    }

    @Override
    public SavedSearchPromptLog getSavedSearchPromptLog(Mailbox mbox) throws ServiceException {
        String key = mbox.getAccountId();
        SavedSearchPromptLog promptLog = promptLogCache.get(key);
        if (promptLog == null) {
            promptLog = new InMemorySavedSearchPromptLog();
            promptLogCache.put(key, promptLog);
        }
        return promptLog;
    }
}
