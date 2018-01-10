package com.zimbra.cs.ml.callback;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.mailbox.MailItem;
import com.zimbra.cs.mailbox.Message;
import com.zimbra.cs.mailbox.SmartFolder;

public class BinaryExclusiveSmartFolderCallback extends ExclusiveClassCallback<Message> {

    private String positiveClassLabel;
    private String smartFolderTagName;

    public BinaryExclusiveSmartFolderCallback(String positiveClassLabel, String smartFolderName) {
        this.positiveClassLabel = positiveClassLabel;
        this.smartFolderTagName = SmartFolder.getInternalTagName(smartFolderName);
    }

    @Override
    public void handle(Message msg, String exclusiveClassName) throws ServiceException {
        if (exclusiveClassName.equalsIgnoreCase(positiveClassLabel)) {
            msg.getMailbox().alterTag(null, msg.getId(), MailItem.Type.MESSAGE, smartFolderTagName, true, null);
        }
    }
}
