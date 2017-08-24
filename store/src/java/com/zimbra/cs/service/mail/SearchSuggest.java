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
        Integer num = req.getLimit();
        String query = req.getQuery();
        SearchHistoryParams params = new SearchHistoryParams();
        if (num != null) {
            params.setNumResults(num);
        }
        if (!Strings.isNullOrEmpty(query)) {
            params.setPrefix(query);
        }
        SearchSuggestResponse resp = new SearchSuggestResponse();
        SearchHistoryStore store = SearchHistoryStore.getInstance();
        resp.setSearches(store.getHistory(mbox, params));
        return zsc.jaxbToElement(resp);
    }
}
