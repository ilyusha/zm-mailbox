/*
 * ***** BEGIN LICENSE BLOCK *****
 * Zimbra Collaboration Suite Server
 * Copyright (C) 2015 Zimbra, Inc.
 * 
 * The contents of this file are subject to the Zimbra Public License
 * Version 1.3 ("License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://www.zimbra.com/license.
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
 * ***** END LICENSE BLOCK *****
 */

package com.zimbra.cs.service.account.zmg;

import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.soap.Element;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Account;
import com.zimbra.cs.account.ZmgDevice;
import com.zimbra.cs.mailbox.Mailbox;
import com.zimbra.cs.mailbox.MailboxManager;
import com.zimbra.cs.service.account.AccountDocumentHandler;
import com.zimbra.soap.JaxbUtil;
import com.zimbra.soap.ZimbraSoapContext;
import com.zimbra.soap.account.message.RegisterMobileGatewayAppRequest;
import com.zimbra.soap.account.message.RegisterMobileGatewayAppResponse;
import com.zimbra.soap.account.type.ZmgDeviceSpec;

public class RegisterMobileGatewayApp extends AccountDocumentHandler {

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.zimbra.soap.DocumentHandler#handle(com.zimbra.common.soap.Element,
     * java.util.Map)
     */
    @Override
    public Element handle(Element request, Map<String, Object> context) throws ServiceException {
        ZimbraSoapContext zsc = getZimbraSoapContext(context);
        Account account = getRequestedAccount(zsc);

        if (!canAccessAccount(zsc, account)) {
            throw ServiceException.PERM_DENIED("can not access account");
        }

        RegisterMobileGatewayAppRequest req = JaxbUtil.elementToJaxb(request);
        ZmgDeviceSpec device = req.getZmgDevice();
        if (device == null || device.getAppId() == null || device.getRegistrationId() == null
            || device.getPushProvider() == null) {
            ZimbraLog.mailbox.info("ZMG: Missing required attributes for adding new device");
            throw ServiceException.INVALID_REQUEST(
                "Missing required attributes for adding new device", null);
        }
        Mailbox mbox = MailboxManager.getInstance().getMailboxByAccount(account);
        int result = ZmgDevice.add(mbox.getId(), device);

        if (result >= 1) {
            if (!account.isPrefZmgPushNotificationEnabled()) {
                account.setPrefZmgPushNotificationEnabled(true);
                ZimbraLog.mailbox.info(
                    "ZMG: New device added and Push Notification enabled. Token=%s",
                    device.getRegistrationId());
            } else {
                ZimbraLog.mailbox.info("ZMG: New device added. Token=%s",
                    device.getRegistrationId());
            }
        } else {
            ZimbraLog.mailbox.info("ZMG: No new device added");
        }

        RegisterMobileGatewayAppResponse resp = new RegisterMobileGatewayAppResponse();
        return zsc.jaxbToElement(resp);
    }
}
