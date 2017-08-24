package com.zimbra.cs.index.history;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.mailbox.Mailbox;

/**
 * In-memory implementation of the config provider that bypasses LDAP.
 * Used for testing and debugging.
 * @author iraykin
 *
 */
public class InMemorySearchHistoryConfig implements SearchHistoryStore.HistoryConfig {

    private long maxAge;

    public InMemorySearchHistoryConfig(long maxAge) {
        this.maxAge = maxAge;
    }

    @Override
    public long getMaxAge() throws ServiceException {
        return maxAge;
    }

}
