package com.zimbra.soap.mail.message;

import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;

import com.zimbra.common.soap.MailConstants;

@XmlRootElement(name=MailConstants.E_GET_RELATED_CONTACTS_REQUEST)
public class GetRelatedContactsRequest {

    /**
     * @zm-api-field-description The maximum number of results to return. Defaults to 10 if not specified; capped at 100.
     */
    @XmlAttribute(name=MailConstants.A_LIMIT, required=false)
    private Integer limit;

    /**
     * @zm-api-field-description The type of contact affinity to use. If not specified,
     * the combined affinity will be used.
     */
    @XmlAttribute(name=MailConstants.A_CONTACT_AFFINITY_TYPE, required=false)
    private String type;

    /**
     * @zm-api-field-description Optional data source ID to filter recommendations on
     */
    @XmlAttribute(name=MailConstants.A_DATASOURCE_ID, required=false)
    private String dsId;

    @XmlElements({
        @XmlElement(name=MailConstants.E_CONTACT, type=String.class, required=true),
    })
    private List<String> contacts;

    public GetRelatedContactsRequest() {}

    public GetRelatedContactsRequest(int limit) {
        this.limit = limit;
    }

    public List<String> getContacts() { return contacts; }
    public void setContacts(List<String> contacts) { this.contacts = contacts; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public Integer getLimit() { return limit; }
    public void setLimit(int limit) { this.limit = limit; }

    public String getDataSourceId() { return dsId; }
    public void setDataSourceId(String dsId) { this.dsId = dsId; }
}
