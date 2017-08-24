package com.zimbra.cs.service.mail;

import java.util.Map;

import com.google.common.base.Strings;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.soap.Element;
import com.zimbra.cs.index.history.SearchHistoryStore;
import com.zimbra.cs.index.history.SearchHistoryStore.SearchHistoryParams;
import com.zimbra.cs.mailbox.Mailbox;
import com.zimbra.soap.ZimbraSoapContext;
import com.zimbra.soap.mail.message.SearchSuggestRequest;
import com.zimbra.soap.mail.message.SearchSuggestResponse;

public class SearchSuggest extends MailDocumentHandler {

    @Override
    public Element handle(Element request, Map<String, Object> context)
            throws ServiceException {
        ZimbraSoapContext zsc = getZimbraSoapContext(context);
        Mailbox mbox = getRequestedMailbox(zsc);
        SearchSuggestRequest req = zsc.elementToJaxb(request);
        SearchHistoryParams params = getSearchHistoryParams(req);
        SearchSuggestResponse resp = new SearchSuggestResponse();
        SearchHistoryStore store = SearchHistoryStore.getInstance();
        resp.setSearches(store.getHistory(mbox, params));
        return zsc.jaxbToElement(resp);
    }

    private SearchHistoryParams getSearchHistoryParams(SearchSuggestRequest req) {
        SearchHistoryParams params = new SearchHistoryParams();
        String query = req.getQuery();
        if (!Strings.isNullOrEmpty(query)) {
            params.setPrefix(query);
        }
        //TODO: get result limit from LDAP
        params.setNumResults(5);
        return params;
    }
}