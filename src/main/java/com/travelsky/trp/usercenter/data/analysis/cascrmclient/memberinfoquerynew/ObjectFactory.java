
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _GetMemberInfoNewResponse_QNAME = new QName("http://service.memberInfoQueryNew.crm_interface.sda.com/", "getMemberInfoNewResponse");
    private final static QName _GetMemberInfoNew_QNAME = new QName("http://service.memberInfoQueryNew.crm_interface.sda.com/", "getMemberInfoNew");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link GetMemberInfoNewResponse }
     * 
     */
    public GetMemberInfoNewResponse createGetMemberInfoNewResponse() {
        return new GetMemberInfoNewResponse();
    }

    /**
     * Create an instance of {@link GetMemberInfoNew }
     * 
     */
    public GetMemberInfoNew createGetMemberInfoNew() {
        return new GetMemberInfoNew();
    }

    /**
     * Create an instance of {@link MemberInfoQueryNewInputEntity }
     * 
     */
    public MemberInfoQueryNewInputEntity createMemberInfoQueryNewInputEntity() {
        return new MemberInfoQueryNewInputEntity();
    }

    /**
     * Create an instance of {@link MemberInfo }
     * 
     */
    public MemberInfo createMemberInfo() {
        return new MemberInfo();
    }

    /**
     * Create an instance of {@link Credential }
     * 
     */
    public Credential createCredential() {
        return new Credential();
    }

    /**
     * Create an instance of {@link Member }
     * 
     */
    public Member createMember() {
        return new Member();
    }

    /**
     * Create an instance of {@link ListOfCrmLoyMemberNew }
     * 
     */
    public ListOfCrmLoyMemberNew createListOfCrmLoyMemberNew() {
        return new ListOfCrmLoyMemberNew();
    }

    /**
     * Create an instance of {@link CredentialList }
     * 
     */
    public CredentialList createCredentialList() {
        return new CredentialList();
    }

    /**
     * Create an instance of {@link MemberInfoQueryNewOutputEntity }
     * 
     */
    public MemberInfoQueryNewOutputEntity createMemberInfoQueryNewOutputEntity() {
        return new MemberInfoQueryNewOutputEntity();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetMemberInfoNewResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://service.memberInfoQueryNew.crm_interface.sda.com/", name = "getMemberInfoNewResponse")
    public JAXBElement<GetMemberInfoNewResponse> createGetMemberInfoNewResponse(GetMemberInfoNewResponse value) {
        return new JAXBElement<GetMemberInfoNewResponse>(_GetMemberInfoNewResponse_QNAME, GetMemberInfoNewResponse.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetMemberInfoNew }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://service.memberInfoQueryNew.crm_interface.sda.com/", name = "getMemberInfoNew")
    public JAXBElement<GetMemberInfoNew> createGetMemberInfoNew(GetMemberInfoNew value) {
        return new JAXBElement<GetMemberInfoNew>(_GetMemberInfoNew_QNAME, GetMemberInfoNew.class, null, value);
    }

}
