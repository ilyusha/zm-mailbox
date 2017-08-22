package com.zimbra.cs.index.history;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.mailbox.Mailbox;

public class SearchHistoryStore {

    private static Factory factory;
    private static SearchHistoryStore instance;

    static {
        setFactory(InMemorySearchHistoryFactory.class);
    }

    public static synchronized SearchHistoryStore getInstance() {
        if (instance == null) {
            synchronized (SearchHistoryStore.class) {
                if (instance == null) {
                    instance = new SearchHistoryStore();
                }
            }
        }
        return instance;
    }

    public static void setFactory(Class<? extends Factory> factoryClass) {

        Factory factory;
        try {
            factory = factoryClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            ZimbraLog.ephemeral.error("unable to instantiate SearchHistoryStore.Factory %s", factoryClass, e);
            factory = new InMemorySearchHistoryFactory();
        }
        SearchHistoryStore.factory = factory;
    }

    /**
     * Add a query string to the search history
     * @param query
     * @throws ServiceException
     */
    public void add(Mailbox mbox, String searchString) throws ServiceException {
        add(mbox, searchString, System.currentTimeMillis());
        }

    private HistoryMetadataStore getMetadata(Mailbox mbox) {
        return factory.getMetadataStore(mbox);
    }

    private HistoryIndex getIndex(Mailbox mbox) {
        return factory.getIndex(mbox);
    }

    @VisibleForTesting
    void add(Mailbox mbox, String searchString, long millis) throws ServiceException {
        HistoryMetadataStore mdStore = getMetadata(mbox);
        HistoryIndex index = getIndex(mbox);
        if (mdStore.exists(searchString)) {
            mdStore.update(searchString, millis);
        } else {
            int id = mdStore.add(searchString, millis);
            index.add(id, searchString);
        }
    }

    /**
     * Remove old search history
     * @param params
     * @throws ServiceException
     */
    public void pruneHistory(Mailbox mbox, PruneParams params) throws ServiceException {
        HistoryMetadataStore mdStore = getMetadata(mbox);
        HistoryIndex index = getIndex(mbox);
        if (params.isDeleteAll()) {
            index.deleteAll();
            mdStore.deleteAll();
        } else {
            Collection<Integer> ids = mdStore.delete(params);
            index.delete(ids);
        }
    }

    /**
     * Get the the search history for the given parameters
     * @param params
     * @return
     * @throws ServiceException
     */
    public List<String> getHistory(Mailbox mbox, SearchHistoryParams params) throws ServiceException {
        SearchHistoryMetadataParams mdParams = SearchHistoryMetadataParams.fromSearchParams(params);
        HistoryMetadataStore mdStore = getMetadata(mbox);
        HistoryIndex index = getIndex(mbox);
        if(params.hasPrefix()) {
            String prefix = params.getPrefix();
            Collection<Integer> ids = index.search(prefix);
            if (ids.isEmpty()) {
                return Collections.emptyList();
            } else {
                mdParams.setIds(ids);
            }
        }
        return mdStore.search(mdParams);
    }

    /**
     * Get the number of times the given term was searched
     */
    public int getCount(Mailbox mbox, String searchString) throws ServiceException {
        HistoryMetadataStore mdStore = getMetadata(mbox);
        return mdStore.getCount(searchString, -1);
    }

    /**
     * Get the number of times the given term was searched within the given timeframe
     */
    public int getCount(Mailbox mbox, String searchString, long maxAgeMillis) throws ServiceException {
        HistoryMetadataStore mdStore = getMetadata(mbox);
        return mdStore.getCount(searchString, maxAgeMillis);
    }


    public static interface Factory {
        public HistoryIndex getIndex(Mailbox mbox);
        public HistoryMetadataStore getMetadataStore(Mailbox mbox);
    }

    public static class SearchHistoryMetadataParams {
        private int numResults = 0;
        private long maxAge = 0;
        private Collection<Integer> ids;

        public SearchHistoryMetadataParams() {
            this(0, 0L);
        }

        public SearchHistoryMetadataParams(int numResults) {
            this(numResults, 0L);
        }

        public SearchHistoryMetadataParams(int numResults, long maxAge) {
            this.numResults = numResults;
            this.maxAge = maxAge;
        }

        public int getNumResults() {
            return numResults;
        }

        public Long getMaxAge() {
            return maxAge;
        }


        public void setNumResults(int num) {
            numResults = num;
        }

        public void setMaxAge(long millis) {
            maxAge = millis;
        }

        public void setIds(Collection<Integer> ids) {
            this.ids = ids;
        }

        public Collection<Integer> getIds() {
            return ids;
        }

        public boolean hasIds() {
            return ids != null && !ids.isEmpty();
        }

        public boolean hasMaxAge() {
            return maxAge > 0;
        }

        public static SearchHistoryMetadataParams fromSearchParams(SearchHistoryParams params) {
            return new SearchHistoryMetadataParams(params.getNumResults(), params.getMaxAge());
        }
    }

    public static class SearchHistoryParams extends SearchHistoryMetadataParams {

        private String prefix;

        public SearchHistoryParams() {
            this(0, 0L, null);
        }
        public SearchHistoryParams(int numResults, Long maxAge, String prefix) {
            super(numResults, maxAge);
            this.prefix = prefix;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        public boolean hasPrefix() {
            return !Strings.isNullOrEmpty(prefix);
        }

        public String getPrefix() {
            return prefix;
        }
    }

    /**
     * Parameters governing pruning old searches from the history
     */
    public static class PruneParams {
        private int keepCount = -1; //keep up to n distinct searches
        private long maxAge = -1; //prune all searches older than this
        private boolean all = false; //clear search history

        public static PruneParams pruneByAge(long maxAge) {
            PruneParams params = new PruneParams();
            params.maxAge = maxAge;
            return params;
        }

        public static PruneParams pruneByCount(int keepCount) {
            PruneParams params = new PruneParams();
            params.keepCount = keepCount;
            return params;
        }

        public static PruneParams pruneAll() {
            PruneParams params = new PruneParams();
            params.all = true;
            return params;
        }

        public long getMaxAge() {
            return maxAge;
        }
        public int getKeepCount() {
            return keepCount;
        }
        public boolean isDeleteAll() {
            return all;
        }
    }

    /**
     * Searchable index of search history entries.
     * The index only returns IDs of entries, the actual string must be looked up in
     * the metadata store.
     */
    public static interface HistoryIndex {

        /**
         * Add a search history entry to the index with the given ID
         */
        public void add(int id, String entry) throws ServiceException;

        /**
         * Search the index for IDs of matching entries
         */
        public Collection<Integer> search(String searchString) throws ServiceException;

        /**
         * Delete entries with the given ids from the index
         */
        public void delete(Collection<Integer> ids) throws ServiceException;

        /**
         * Delete all search history data from the index
         */
        public void deleteAll() throws ServiceException;
    }

    /**
     * Metadata store for search history entries. Does not support text searches;
     * this must be done by the HistoryIndex.
     */
    public static interface HistoryMetadataStore {

        /**
         * Store a search history entry in the metadata store.
         * Returns the ID of the entry.
         */
        public int add(String entry, long timestamp) throws ServiceException;

        /**
         * Search the metadata store for matching entries
         */
        public List<String> search(SearchHistoryMetadataParams params) throws ServiceException;

        /**
         * Determine whether this entry already exists in the store
         */
        public boolean exists(String searchString) throws ServiceException;

        /**
         * Update the existing metadata entry with a new timestamp
         */
        public void update(String searchString, long millis) throws ServiceException;

        /**
         * Delete entries with the given parameters from the metadata store and
         * return the IDs to be deleted from the index
         */
        public Collection<Integer> delete(PruneParams params) throws ServiceException;

        /**
         * Delete all search history
         */
        public void deleteAll() throws ServiceException;

        /**
         * Get the number of times the given term was searched within the given timeframe
         */
        public int getCount(String searchString, long maxAgeMillis)  throws ServiceException;
    }
}
