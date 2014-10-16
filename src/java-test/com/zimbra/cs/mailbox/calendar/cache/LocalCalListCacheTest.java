/*
 * ***** BEGIN LICENSE BLOCK *****
 * Zimbra Collaboration Suite Server
 * Copyright (C) 2014 Zimbra Software, LLC.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software Foundation,
 * version 2 of the License.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
 * ***** END LICENSE BLOCK *****
 */
package com.zimbra.cs.mailbox.calendar.cache;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.util.Zimbra;

/**
 * Unit test for {@link LocalCalListCache}.
 */
public final class LocalCalListCacheTest extends AbstractCalListCacheTest {

    @Override
    protected CalListCache constructCache() throws ServiceException {
        CalListCache cache = new LocalCalListCache();
        Zimbra.getAppContext().getAutowireCapableBeanFactory().autowireBean(cache);
        Zimbra.getAppContext().getAutowireCapableBeanFactory().initializeBean(cache, "calListCache");
        return cache;
    }

    @Override
    protected boolean isExternalCacheAvailableForTest() throws Exception {
        return true;
    }

    @Override
    protected void flushCacheBetweenTests() throws Exception {
        ((LocalCalListCache)cache).flush();
    }
}
