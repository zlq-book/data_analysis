
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.querycard;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the com.travelsky.trp.usercenter.data.analysis.cascrmclient.querycard package. 
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

    private final static QName _GetQueryCardInfo_QNAME = new QName("http://service.QueryCard.crm_interface.sda.com/", "getQueryCardInfo");
    private final static QName _GetQueryCardInfoResponse_QNAME = new QName("http://service.QueryCard.crm_interface.sda.com/", "getQueryCardInfoResponse");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: com.travelsky.trp.usercenter.data.analysis.cascrmclient.querycard
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link GetQueryCardInfoResponse }
     * 
     */
    public GetQueryCardInfoResponse createGetQueryCardInfoResponse() {
        return new GetQueryCardInfoResponse();
    }

    /**
     * Create an instance of {@link GetQueryCardInfo }
     * 
     */
    public GetQueryCardInfo createGetQueryCardInfo() {
        return new GetQueryCardInfo();
    }

    /**
     * Create an instance of {@link MemberInfo }
     * 
     */
    public MemberInfo createMemberInfo() {
        return new MemberInfo();
    }

    /**
     * Create an instance of {@link MemberInfoList }
     * 
     */
    public MemberInfoList createMemberInfoList() {
        return new MemberInfoList();
    }

    /**
     * Create an instance of {@link QueryCardInputEntity }
     * 
     */
    public QueryCardInputEntity createQueryCardInputEntity() {
        return new QueryCardInputEntity();
    }

    /**
     * Create an instance of {@link QueryCardOutputEntity }
     * 
     */
    public QueryCardOutputEntity createQueryCardOutputEntity() {
        return new QueryCardOutputEntity();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetQueryCardInfo }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://service.QueryCard.crm_interface.sda.com/", name = "getQueryCardInfo")
    public JAXBElement<GetQueryCardInfo> createGetQueryCardInfo(GetQueryCardInfo value) {
        return new JAXBElement<GetQueryCardInfo>(_GetQueryCardInfo_QNAME, GetQueryCardInfo.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link GetQueryCardInfoResponse }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://service.QueryCard.crm_interface.sda.com/", name = "getQueryCardInfoResponse")
    public JAXBElement<GetQueryCardInfoResponse> createGetQueryCardInfoResponse(GetQueryCardInfoResponse value) {
        return new JAXBElement<GetQueryCardInfoResponse>(_GetQueryCardInfoResponse_QNAME, GetQueryCardInfoResponse.class, null, value);
    }

}
