package com.zimbra.cs.index.history;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.index.history.SearchHistoryStore.PruneParams;
import com.zimbra.cs.index.history.SearchHistoryStore.SearchHistoryMetadataParams;

/**
 * In-memory implementation of the search history index.
 * Should only be used for testing or debugging.
 */
public class InMemorySearchHistoryMetadata implements SearchHistoryStore.HistoryMetadataStore {

    private int lastID = 0;
    //ids of search entries
    private Map<String, Integer> idMap = new HashMap<String, Integer>();
    //running list of search entries
    private List<EntryInfo> history = new LinkedList<EntryInfo>();
    //used for groupBy functionality
    private HashMultimap<String, EntryInfo> buckets = HashMultimap.create();


    @Override
    public int add(String searchString, long timestamp) throws ServiceException {
        if (idMap.containsKey(searchString)) {
            throw ServiceException.FAILURE(searchString + " already exists", null);
        }
        int newId = ++lastID;
        idMap.put(searchString, newId);
        ZimbraLog.search.debug("added new search history entry %s with id %s", searchString, newId);
        EntryInfo info = new EntryInfo(newId, searchString, timestamp);
        history.add(0, info);
        buckets.put(searchString, info);
        lastID = newId;
        return newId;
    }

    @Override
    public List<String> search(SearchHistoryMetadataParams params)
            throws ServiceException {
        int numResults = params.getNumResults();

        Predicate<EntryInfo> filter = new Predicate<EntryInfo>() {
            private Set<Integer> seen = new HashSet<Integer>();

            @Override
            public boolean test(EntryInfo info) {
                boolean isFirstOccurrence = !seen.contains(info.getID());
                seen.add(info.getID());
                return isFirstOccurrence;
            }
        };

        if (params.hasIds()) {
            Set<Integer> ids = new HashSet<Integer>(params.getIds());
            ZimbraLog.search.debug("constructing idFilter with ids=%s", Joiner.on(",").join(ids));
            Predicate<EntryInfo> idFilter = new Predicate<EntryInfo>() {

                @Override
                public boolean test(EntryInfo info) {
                    if (!ids.contains(info.getID())) {
                        ZimbraLog.search.debug("rejecting '%s' (id=%s, outside requested ID set)", info.getSearchString(), info.getID());
                        return false;
                    } else {
                        return true;
                    }
                }
            };
            filter = filter.and(idFilter);
        }

        if (params.hasMaxAge()) {
            long now = System.currentTimeMillis();
            long maxAge = params.getMaxAge();
            ZimbraLog.search.debug("constructing ageFilter with age=%s", maxAge);
            Predicate<EntryInfo> ageFilter = new Predicate<EntryInfo>() {

                @Override
                public boolean test(EntryInfo info) {
                    long age = now - info.getTimestamp();
                    if (age < maxAge) {
                        return true;
                    } else {
                        ZimbraLog.search.debug("rejecting '%s' (age=%s ms)", info.getSearchString(), age);
                        return false;
                    }
                }
            };
            filter = filter.and(ageFilter);
        }

        if (numResults > 0) {
            ZimbraLog.search.debug("constructing numResultsFilter with n=%s", numResults);
            Predicate<EntryInfo> numResultsFilter = new Predicate<EntryInfo>() {
                int numResultsSeen = 0;
                @Override
                public boolean test(EntryInfo info) {
                    numResultsSeen++;
                    if (numResultsSeen <= numResults) {
                        return true;
                    } else {
                        ZimbraLog.search.debug("rejecting '%s' (result #%s, max=%s)", info.getSearchString(), numResultsSeen, numResults);
                        return false;
                    }
                }
            };
            filter = filter.and(numResultsFilter);
        }

        List<String> results = new ArrayList<String>();
        history.stream().filter(filter)
        .map(info -> info.getSearchString())
        .forEach(results::add);
        return results;
    }

    @Override
    public boolean exists(String query) throws ServiceException {
        return idMap.containsKey(query);
    }

    @Override
    public void update(String searchString, long timestamp) throws ServiceException {
        int id = idMap.get(searchString);
        EntryInfo info = new EntryInfo(id, searchString, timestamp);
        history.add(0, info);
        buckets.put(searchString, info);
    }

    private List<Integer> deleteByPartition(int index) {
        List<EntryInfo> toKeep = history.subList(0, index);
        List<EntryInfo> toDelete = history.subList(index, history.size());
        ZimbraLog.search.debug("marking %s entries to delete past index %s", toDelete.size(), index);
        for (EntryInfo info: toDelete) {
            buckets.get(info.getSearchString()).removeIf(item -> item.timestamp == info.getTimestamp());
        }
        //we don't want to delete entries from the index if they still exist in the remaining history,
        //so we filter by that before returning
        List<Integer> idsToDelete = new ArrayList<Integer>();
        toDelete.stream().filter(info -> buckets.get(info.searchString).isEmpty())
        .map(info -> info.id)
        .forEach(idsToDelete::add);
        history = toKeep;
        return idsToDelete;
    }

    private List<Integer> deleteByCount(int keepCount) {
        ZimbraLog.search.debug("pruning search history with n=%s",keepCount);
        Set<String> distinctSearches = new HashSet<String>();
        int idx = 0;
        for (EntryInfo info: history) {
            distinctSearches.add(info.getSearchString());
            idx++;
            if (distinctSearches.size() == keepCount) {
                return deleteByPartition(idx);
            }
        }
        return Collections.emptyList();
    }

    private List<Integer> deleteByAge(long maxAge) {
        ZimbraLog.search.debug("pruning search history with max age=%s",maxAge);
        int idx = 0;
        long now = System.currentTimeMillis();
        for (EntryInfo info: history) {
            if (now - info.timestamp > maxAge) {
                return deleteByPartition(idx);
            }
            idx++;
        }
        return Collections.emptyList();
    }

    @Override
    public Collection<Integer> delete(PruneParams params) throws ServiceException {
        int keepCount = params.getKeepCount();
        if (keepCount > 0) {
            return deleteByCount(keepCount);
        } else {
            return deleteByAge(params.getMaxAge());
        }
    }

    @Override
    public void deleteAll() throws ServiceException {
        idMap.clear();
        history.clear();
        buckets.clear();
    }

    @Override
    public int getCount(String searchString, long maxAge)
            throws ServiceException {
        Set<EntryInfo> instances = buckets.get(searchString);
        if(maxAge > 0) {
            //exclude expired entries
            long now = System.currentTimeMillis();
            return (int) instances.stream().filter(i -> now - i.timestamp < maxAge).count();
        } else {
            return instances.size();
        }
    }

    private static class EntryInfo {
        private int id;
        private String searchString;
        private Long timestamp;
        public EntryInfo(int id, String searchString, long timestamp) {
            this.id = id;
            this.searchString = searchString;
            this.timestamp = timestamp;
        }

        public String getSearchString() { return searchString; }
        public long getTimestamp() { return timestamp; }
        public int getID() { return id; }

    }
}
