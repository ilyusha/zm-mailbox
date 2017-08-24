package com.zimbra.soap.mail.message;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import com.zimbra.common.soap.MailConstants;

@XmlRootElement(name=MailConstants.E_SEARCH_SUGGEST_REQUEST)
public final class SearchSuggestRequest {

    public SearchSuggestRequest() {}

    public SearchSuggestRequest(String query) {
        this.query = query;
    }

    /**
     * @zm-api-field-tag query
     * @zm-api-field-description The search query to autocomplete
     */
    @XmlAttribute(name=MailConstants.A_QUERY, required=false)
    private String query;

    public void setQuery(String query) { this.query = query; }
    public String getQuery() { return query; }

}
