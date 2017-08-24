package com.zimbra.cs.index.history;

import java.util.HashMap;
import java.util.Map;

import com.zimbra.cs.index.history.SearchHistoryStore.HistoryConfig;
import com.zimbra.cs.index.history.SearchHistoryStore.HistoryIndex;
import com.zimbra.cs.index.history.SearchHistoryStore.HistoryMetadataStore;
import com.zimbra.cs.index.history.SearchHistoryStore.SavedSearchPromptLog;
import com.zimbra.cs.mailbox.Mailbox;

public class InMemorySearchHistoryFactory implements SearchHistoryStore.Factory {

    private Map<String, HistoryIndex> indexCache = new HashMap<String, HistoryIndex>();
    private Map<String, HistoryMetadataStore> mdCache = new HashMap<String, HistoryMetadataStore>();
    private Map<String, SavedSearchPromptLog> promptLogCache = new HashMap<String, SavedSearchPromptLog>();
    private static long maxAge = 0;

    public static void setMaxAge(long maxAge) {
        InMemorySearchHistoryFactory.maxAge = maxAge;
    }

    @Override
    public HistoryIndex getIndex(Mailbox mbox) {
        String key = mbox.getAccountId();
        HistoryIndex index = indexCache.get(key);
        if (index == null) {
            index = new InMemorySearchHistoryIndex();
            indexCache.put(key, index);
        }
        return index;
    }

    @Override
    public HistoryMetadataStore getMetadataStore(Mailbox mbox) {
        String key = mbox.getAccountId();
        HistoryMetadataStore mdStore = mdCache.get(key);
        if (mdStore == null) {
            mdStore = new InMemorySearchHistoryMetadata();
            mdCache.put(key, mdStore);
        }
        return mdStore;
    }

    @Override
    public HistoryConfig getConfig(Mailbox mbox) {
        return new InMemorySearchHistoryConfig(maxAge);
    }

    @Override
    public SavedSearchPromptLog getSavedSearchPromptLog(Mailbox mbox) {
        String key = mbox.getAccountId();
        SavedSearchPromptLog promptLog = promptLogCache.get(key);
        if (promptLog == null) {
            promptLog = new InMemorySavedSearchPromptLog();
            promptLogCache.put(key, promptLog);
        }
        return promptLog;
    }
}
