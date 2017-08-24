package com.zimbra.cs.service.mail;

import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.soap.Element;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.index.history.SearchHistoryStore;
import com.zimbra.cs.index.history.SearchHistoryStore.SavedSearchStatus;
import com.zimbra.cs.mailbox.Mailbox;
import com.zimbra.soap.ZimbraSoapContext;
import com.zimbra.soap.mail.message.RejectSaveSearchPromptRequest;
import com.zimbra.soap.mail.message.RejectSaveSearchPromptResponse;

public class RejectSaveSearchPrompt extends MailDocumentHandler {

    @Override
    public Element handle(Element request, Map<String, Object> context)
            throws ServiceException {
        ZimbraSoapContext zsc = getZimbraSoapContext(context);
        Mailbox mbox = getRequestedMailbox(zsc);
        RejectSaveSearchPromptRequest req = zsc.elementToJaxb(request);
        String query = req.getQuery();
        SearchHistoryStore store = SearchHistoryStore.getInstance();
        if (store.savedSearchPrompted(mbox, query)) {
            store.setSavedSearchPromptStatus(mbox, query, SavedSearchStatus.REJECTED);
        } else {
            ZimbraLog.search.warn("received RejectSaveSearchPrompt for query '%s' but no prompt has been issued", query);
        }
        return zsc.jaxbToElement(new RejectSaveSearchPromptResponse());
    }

}
