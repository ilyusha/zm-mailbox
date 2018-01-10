package com.zimbra.cs.ml.callback;

import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.mailbox.MailItem;
import com.zimbra.cs.mailbox.Message;
import com.zimbra.cs.mailbox.SmartFolder;

public class MultilabelExclusiveSmartFolderCallback extends ExclusiveClassCallback<Message> {

    private Map<String, String> smartFolderNameMap;

    public MultilabelExclusiveSmartFolderCallback(Map<String, String> smartFolderNameMap) {
        this.smartFolderNameMap = smartFolderNameMap;
    }

    @Override
    public void handle(Message msg, String exclusiveClassName) throws ServiceException {
        String smartFolderName = smartFolderNameMap.get(exclusiveClassName);
        if (smartFolderName == null) {
            ZimbraLog.ml.warn("ExclusiveSmartFolderCallback encountered unknown exclusive class label \"%s\"", exclusiveClassName);
        } else {
            msg.getMailbox().alterTag(null, msg.getId(), MailItem.Type.MESSAGE, SmartFolder.getInternalTagName(smartFolderName), true, null);
        }
    }

}
