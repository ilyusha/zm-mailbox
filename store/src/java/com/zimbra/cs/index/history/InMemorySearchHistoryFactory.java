package com.zimbra.cs.index.history;

import java.util.HashMap;
import java.util.Map;

import com.zimbra.cs.index.history.SearchHistoryStore.HistoryConfig;
import com.zimbra.cs.index.history.SearchHistoryStore.HistoryIndex;
import com.zimbra.cs.index.history.SearchHistoryStore.HistoryMetadataStore;
import com.zimbra.cs.mailbox.Mailbox;

public class InMemorySearchHistoryFactory implements SearchHistoryStore.Factory {

    private Map<String, HistoryIndex> indexCache;
    private Map<String, HistoryMetadataStore> mdCache;
    private static long maxAge = 0;

    public static void setMaxAge(long maxAge) {
        InMemorySearchHistoryFactory.maxAge = maxAge;
    }

    public InMemorySearchHistoryFactory() {
        indexCache = new HashMap<String, HistoryIndex>();
        mdCache = new HashMap<String, HistoryMetadataStore>();
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
}
