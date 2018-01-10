package com.zimbra.cs.mailbox;

import java.util.Set;

import com.google.common.collect.Sets;
import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.account.Server;

public class LdapSmartFolderProvider extends SmartFolderProvider {

    @Override
    public Set<String> getSmartFolderNames() throws ServiceException {
        Server server = Provisioning.getInstance().getLocalServer();
        Set<String> exclusiveFolders = Sets.newHashSet(server.getExclusiveSmartFolders());
        Set<String> overlappingFolders = Sets.newHashSet(server.getOverlappingSmartFolders());
        return Sets.union(exclusiveFolders, overlappingFolders);
    }
}
