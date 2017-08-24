package com.zimbra.cs.service.mail;

import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.soap.Element;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.index.history.SearchHistoryStore;
import com.zimbra.cs.mailbox.Mailbox;
import com.zimbra.soap.ZimbraSoapContext;
import com.zimbra.soap.mail.message.ClearSearchHistoryResponse;

public class ClearSearchHistory extends MailDocumentHandler {

    @Override
    public Element handle(Element request, Map<String, Object> context) throws ServiceException {
    ZimbraSoapContext zsc = getZimbraSoapContext(context);
    Mailbox mbox = getRequestedMailbox(zsc);
    SearchHistoryStore store = SearchHistoryStore.getInstance();
    ZimbraLog.search.info("deleting search history for account %s", mbox.getAccountId());
    store.deleteHistory(mbox);
    return zsc.jaxbToElement(new ClearSearchHistoryResponse());
    }

}
