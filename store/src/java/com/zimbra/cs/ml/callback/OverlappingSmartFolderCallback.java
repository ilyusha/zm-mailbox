package com.zimbra.cs.ml.callback;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.mailbox.MailItem;
import com.zimbra.cs.mailbox.Message;
import com.zimbra.cs.mailbox.SmartFolder;

public class OverlappingSmartFolderCallback extends OverlappingClassCallback<Message> {

    private String smartFolderTagName;

    public OverlappingSmartFolderCallback(String smartFolderClassName, String smartFolderTagName) {
        super(smartFolderClassName);
        this.smartFolderTagName = SmartFolder.getInternalTagName(smartFolderTagName);
    }

    @Override
    public void handle(Message msg) throws ServiceException {
        msg.getMailbox().alterTag(null, msg.getId(), MailItem.Type.MESSAGE, smartFolderTagName, true, null);
    }
}
