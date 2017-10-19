package com.zimbra.soap.mail.message;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import com.zimbra.common.soap.MailConstants;

@XmlRootElement(name=MailConstants.E_GET_RELATED_CONTACTS_RESPONSE)
public class GetRelatedContactsResponse {

    @XmlElementWrapper(name=MailConstants.E_RELATED_CONTACTS)
    @XmlElement(name=MailConstants.E_CONTACT, type=String.class)
    private List<String> relatedContacts;

    public List<String> getRelatedContacts() {return relatedContacts; }
    public void setRelatedContacts(List<String> contacts) { this.relatedContacts = contacts; }

}
