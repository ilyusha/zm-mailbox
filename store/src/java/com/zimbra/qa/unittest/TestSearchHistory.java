package com.zimbra.qa.unittest;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zimbra.client.ZFolder.Color;
import com.zimbra.client.ZMailbox;
import com.zimbra.client.ZSearchParams;
import com.zimbra.client.ZSearchResult;
import com.zimbra.cs.account.Account;
import com.zimbra.cs.mailbox.Mailbox;
import com.zimbra.soap.type.SearchSortBy;

public class TestSearchHistory {

    private static String USER = "TestSearchHistory-test";
    private Account acct;
    private ZMailbox mbox;
    private int searchesForPrompt = 3;

    @Before
    public void setUp() throws Exception {
        acct = TestUtil.createAccount(USER);
        mbox = TestUtil.getZMailbox(USER);
        searchesForPrompt = acct.getNumSearchesForSavedSearchPrompt();
    }

    @After
    public void tearDown() throws Exception {
        TestUtil.deleteAccountIfExists(USER);
    }

    private void search(String query) throws Exception {
        mbox.search(new ZSearchParams(query));
    }

    private void testResultItem(List<String> results, int index, String expected) {
        String msg = String.format("result %s should be '%s'", index+1, expected);
        assertEquals(msg, expected, results.get(index));
    }

    @Test
    public void testSearchHistory() throws Exception {
        search("first search");
        search("search2");
        search("search3");
        search("search4");
        search("first search");

        List<String> results = mbox.getSearchSuggestions("");
        assertEquals("should see 4 search history results", 4, results.size());
        testResultItem(results, 0, "first search");
        testResultItem(results, 1, "search4");
        testResultItem(results, 2, "search3");
        testResultItem(results, 3, "search2");

        results = mbox.getSearchSuggestions("s");
        assertEquals("should see 3 search history results", 3, results.size());
        testResultItem(results, 0, "search4");
        testResultItem(results, 1, "search3");
        testResultItem(results, 2, "search2");

        assertTrue("no suggestions should be found for non-matching query", mbox.getSearchSuggestions("blah").isEmpty());

        mbox.clearSearchHistory();
        assertTrue("search suggestions should be empty after clearning history", mbox.getSearchSuggestions("").isEmpty());
    }

    private void searchAndTestPrompt(String query, boolean shouldHavePrompt) throws Exception {
        ZSearchResult result = mbox.search(new ZSearchParams(query));
        if (shouldHavePrompt) {
            assertTrue("search response should have saveSearchPrompt", result.hasSavedSearchPrompt());
        } else {
            assertFalse("search response should not have saveSearchPrompt", result.hasSavedSearchPrompt());
        }
    }

    private void doSearchesUpToPrompt() throws Exception {
        for (int i = 0; i < searchesForPrompt - 1; i++) {
            searchAndTestPrompt("testSearch", false);
        }
        searchAndTestPrompt("another search", false); //sanity check - throw in another search
        searchAndTestPrompt("testSearch", true);
    }

    @Test
    public void testSaveSearchPromptNoResponse() throws Exception {
        doSearchesUpToPrompt();
        //another search shouldn't return a prompt if there is one still outstanding
        searchAndTestPrompt("testSearch", false);
    }

    @Test
    public void testSaveSearchPromptRejected() throws Exception {
        doSearchesUpToPrompt();
        mbox.rejectSaveSearchFolderPrompt("testSearch");
        //if a prompt was rejected, further searches shouldn't prompt again
        searchAndTestPrompt("testSearch", false);
    }

    @Test
    public void testSaveSearchPromptAccepted() throws Exception {
        doSearchesUpToPrompt();
        //if a prompt was accepted, further searches shouldn't prompt again
        mbox.createSearchFolder(String.valueOf(Mailbox.ID_FOLDER_INBOX), "search", "testSearch", "message", SearchSortBy.dateAsc, Color.BLUE);
        searchAndTestPrompt("testSearch", false);

    }
}
