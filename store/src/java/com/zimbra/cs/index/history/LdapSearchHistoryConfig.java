package com.zimbra.cs.index.history;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.account.Account;
import com.zimbra.cs.index.history.SearchHistoryStore.HistoryConfig;

public class LdapSearchHistoryConfig implements HistoryConfig {

    private Account acct;

    public LdapSearchHistoryConfig(Account acct) {
        this.acct = acct;
    }

    @Override
    public long getMaxAge() throws ServiceException {
        return acct.getSearchHistoryDuration();
    }
}
