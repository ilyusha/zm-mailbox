package com.zimbra.cs.account.callback;

import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.account.AttributeCallback;
import com.zimbra.cs.account.Entry;
import com.zimbra.cs.contacts.ContactGraph;

public class ContactAffinityCallback extends AttributeCallback {

    @Override
    public void preModify(CallbackContext context, String attrName,
            Object attrValue, Map attrsToModify, Entry entry)
            throws ServiceException {
    }

    @Override
    public void postModify(CallbackContext context, String attrName, Entry entry) {
        //reset the factory so that new configuration can be picked up
        ContactGraph.clearFactory();
    }

}
