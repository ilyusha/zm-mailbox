/*
 * ***** BEGIN LICENSE BLOCK *****
 * Zimbra Collaboration Suite Server
 * Copyright (C) 2016 Synacor, Inc.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software Foundation,
 * version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 * ***** END LICENSE BLOCK *****
 */

package com.zimbra.client;

import java.io.InputStream;
import java.net.URI;

import com.google.common.base.Strings;
import com.zimbra.common.mailbox.ItemIdentifier;
import com.zimbra.common.mailbox.MailItemType;
import com.zimbra.common.mailbox.ZimbraMailItem;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.soap.AccountConstants;

public class ZBaseItem implements ZItem, ZimbraMailItem {

    final String mId;
    String mFlags;
    String mTagIds;
    ZMailbox mMailbox;
    int imapUid;

    public ZBaseItem(String id) {
        mId = id;
    }

    public ZBaseItem(String id, int imapUid) {
        mId = id;
        this.imapUid = imapUid;
    }

    public ZMailbox getMailbox() {
        return mMailbox;
    }

    @Override
    public int getIdInMailbox() throws ServiceException {
        return getIdInMailbox(this.getId());
    }

    public int getIdInMailbox(String id) throws ServiceException {
        String acctId = null;
        try {
            acctId = mMailbox.getAccountId();
        } catch (ServiceException e) {
        }
        ItemIdentifier itemId = new ItemIdentifier(id, acctId);
        return itemId.id;
    }

    @Override
    public String getUuid() {
        return null;
    }

    @Override
    public int getModifiedSequence() {
        throw new UnsupportedOperationException("ZBaseItem method not supported yet");
    }

    public String getFlags() {
        return mFlags;
    }

    public boolean hasFlags() {
        return !Strings.isNullOrEmpty(mFlags);
    }

    /** Returns the "external" flag bitmask, which includes {@link ZItem.Flag#BITMASK_UNREAD} when the item is unread. */
    @Override
    public int getFlagBitmask() {
        return ZItem.Flag.toBitmask(mFlags);
    }

    public boolean hasTags() {
        return !Strings.isNullOrEmpty(mTagIds);
    }

    @Override
    public String[] getTags() {
        String tagIdString = getTagIds();
        return (tagIdString == null) ? new String[0] : tagIdString.split(",");
    }

    public String getTagIds() {
        return mTagIds;
    }

    @Override
    public long getDate() {
        return 0L;
    }
    /** Returns the item's size as it counts against mailbox quota.  For items
     *  that have a blob, this is the size in bytes of the raw blob. */
    @Override
    public long getSize() {
        return 0L;
    }
    @Override
    public MailItemType getMailItemType() {
        return MailItemType.UNKNOWN;
    }
    /** Returns an {@link InputStream} of the raw, uncompressed content of the message.  This is the message body as
     * received via SMTP; no postprocessing has been performed to make opaque attachments (e.g. TNEF) visible.
     *
     * @return The data stream, or <tt>null</tt> if the item has no blob
     * @throws ServiceException when the message file does not exist.
     * @see #getMimeMessage()
     * @see #getContent() */
    @Override
    public InputStream getContentStream() throws ServiceException {
        String contentPath = String.format("%s/get?id=%d", AccountConstants.CONTENT_SERVLET_PATH, getIdInMailbox());
        URI uri = mMailbox.getTransportURI(contentPath);
        return mMailbox.getResource(uri);
    }

    /**
     * @return the UID the item is referenced by in the IMAP server.  Returns <tt>0</tt> for items that require
     * renumbering because of moves.
     * The "IMAP UID" will be the same as the item ID unless the item has been moved after the mailbox owner's first
     * IMAP session. */
    @Override
    public int getImapUid() {
        return imapUid;
    }

    public void setImapUid(int imapUid) {
        this.imapUid = imapUid;
    }

    /** @return item's ID.  IDs are unique within a Mailbox and are assigned in increasing
     * (though not necessarily gap-free) order. */
    @Override
    public String getId() {
        return mId;
    }

    @Override
    public String getAccountId() throws ServiceException {
        return getMailbox().getAccountId();
    }

    @Override
    public int getFolderIdInMailbox() throws ServiceException {
        return 0;
    }
}