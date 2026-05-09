
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.querymemberext;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the com.travelsky.trp.usercenter.data.analysis.cascrmclient.querymemberext package. 
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

    private final static QName _GetQueryMemberExtInfo_QNAME = new QName("http://service.QueryMemberExt.crm_interface.sda.com/", "getQueryMemberExtInfo");
    private final static QName _GetQueryMemberExtInfoResponse_QNAME = new QName("http://service.QueryMemberExt.crm_interface.sda.com/", "getQueryMemberExtInfoResponse");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: com.travelsky.trp.usercenter.data.analysis.cascrmclient.querymemberext
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link GetQueryMemberExtInfoResponse }
     * 
     */
    public GetQueryMemberExtInfoResponse createGetQueryMemberExtInfoResponse() {
        return new GetQueryMemberExtInfoResponse();
    }

    /**
     * Create an instance of {@link GetQueryMemberExtInfo }
     * 
     */
    public GetQueryMemberExtInfo createGetQueryMemberExtInfo() {
        return new GetQueryMemberExtInfo();
    }

    /**
     * Create an instance of {@link AdditionInfo }
     * 
     */
    public AdditionInfo createAdditionInfo() {
        return new AdditionInfo();
    }

    /**
     * Create an instance of {@link Address }
     * 
     */
    public Address createAddress() {
        return new Address();
    }

    /**
     * Create an instance of {@link ListOfCrmLoyMemberMoreInfo }
     * 
     */
    public ListOfCrmLoyMemberMoreInfo createListOfCrmLoyMemberMoreInfo() {
        return new ListOfCrmLoyMemberMoreInfo();
    }

    /**
     * Create an instance of {@link EmailList }
     * 
     */
    public EmailList createEmailList() {
        return new EmailList();
    }

    /**
     * Create an instance of {@link Phone }
     * 
     */
    public Phone createPhone() {
        return new Phone();
    }

    /**
     * Create an instance of {@link AddressList }
     * 
     */
    public AddressList createAddressList() {
        return new AddressList();
    }

    /**
     * Create an instance of {@link QueryMemberExtOutputEntity }
     * 
     */
    public QueryMemberExtOutputEntity createQueryMemberExtOutputEntity() {
        return new QueryMemberExtOutputEntity();
    }

    /**
     * Create an instance of {@link PhoneList }
     * 
     */
    public PhoneList createPhoneList() {
        return new PhoneList();
    }

    /**
     * Create an instance of {@link QueryMemberExtInputEntity }
     * 
     */
    public QueryMemberExtInputEntity createQueryMemberExtInputEntity() {
        return new QueryMemberExtInputEntity();
    }

    /**
     * Create an instance of {@link Email }
     * 
     */
    public Email createEmail() {
        return new Email();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetQueryMemberExtInfo }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://service.QueryMemberExt.crm_interface.sda.com/", name = "getQueryMemberExtInfo")
    public JAXBElement<GetQueryMemberExtInfo> createGetQueryMemberExtInfo(GetQueryMemberExtInfo value) {
        return new JAXBElement<GetQueryMemberExtInfo>(_GetQueryMemberExtInfo_QNAME, GetQueryMemberExtInfo.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetQueryMemberExtInfoResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://service.QueryMemberExt.crm_interface.sda.com/", name = "getQueryMemberExtInfoResponse")
    public JAXBElement<GetQueryMemberExtInfoResponse> createGetQueryMemberExtInfoResponse(GetQueryMemberExtInfoResponse value) {
        return new JAXBElement<GetQueryMemberExtInfoResponse>(_GetQueryMemberExtInfoResponse_QNAME, GetQueryMemberExtInfoResponse.class, null, value);
    }

}
