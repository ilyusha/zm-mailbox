package com.zimbra.cs.service.mail;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.soap.Element;
import com.zimbra.cs.account.Account;
import com.zimbra.cs.contacts.ContactGraph;
import com.zimbra.cs.contacts.ContactGraph.ContactResult;
import com.zimbra.cs.contacts.ContactGraph.ContactsParams;
import com.zimbra.cs.contacts.ContactGraph.EdgeType;
import com.zimbra.soap.ZimbraSoapContext;
import com.zimbra.soap.mail.message.GetRelatedContactsRequest;
import com.zimbra.soap.mail.message.GetRelatedContactsResponse;

public class GetRelatedContacts extends MailDocumentHandler {

    private static final int DEFAULT_CONTACT_SUGGEST_RESULT_LIMIT = 10;
    private static final int MAX_CONTACT_SUGGEST_RESULT_LIMIT = 100;

    @Override
    public Element handle(Element request, Map<String, Object> context)
            throws ServiceException {
        ZimbraSoapContext zsc = getZimbraSoapContext(context);
        Account acct = getRequestedAccount(zsc);
        GetRelatedContactsRequest req = zsc.elementToJaxb(request);
        ContactsParams params = new ContactsParams(req.getContacts(), req.getDataSourceId());
        String typeStr = req.getType();
        if (typeStr == null || typeStr.equalsIgnoreCase("all")) {
            params.setAllEdges();
        } else {
            EdgeType type = EdgeType.getKnownEdgeType(typeStr.toLowerCase());
            params.setEdgeType(type);
            if (type == null) {
                throw ServiceException.INVALID_REQUEST(typeStr + " is not a known contact affinity type", null);
            }
        }
        params.setNumResults(parseLimit(req.getLimit()));
        ContactGraph graph = ContactGraph.getFactory().getContactGraph(acct.getId());
        GetRelatedContactsResponse resp = new GetRelatedContactsResponse();
        List<ContactResult> results = graph.getRelatedContacts(params);
        List<String> contactNames = results.stream().map(ContactResult::getName).collect(Collectors.toList());
        resp.setRelatedContacts(contactNames);
        return zsc.jaxbToElement(resp);
    }

    protected int parseLimit(Integer providedLimit) {
        if (providedLimit == null || providedLimit <= 0) {
            return DEFAULT_CONTACT_SUGGEST_RESULT_LIMIT;
        } else {
            return Math.min(providedLimit, MAX_CONTACT_SUGGEST_RESULT_LIMIT);
        }
    }


}
