
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>memberInfoQueryNewOutputEntity complex type的 Java 类。
 * 
 * <p>以下模式片段指定包含在此类中的预期内容。
 * 
 * <pre>
 * &lt;complexType name="memberInfoQueryNewOutputEntity">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="errorCode" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="errorDesc" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="listOfCrmLoyMemberNew" type="{http://service.memberInfoQueryNew.crm_interface.sda.com/}listOfCrmLoyMemberNew" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "memberInfoQueryNewOutputEntity", propOrder = {
    "errorCode",
    "errorDesc",
    "listOfCrmLoyMemberNew"
})
public class MemberInfoQueryNewOutputEntity {

    protected String errorCode;
    protected String errorDesc;
    protected ListOfCrmLoyMemberNew listOfCrmLoyMemberNew;

    /**
     * 获取errorCode属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * 设置errorCode属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setErrorCode(String value) {
        this.errorCode = value;
    }

    /**
     * 获取errorDesc属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getErrorDesc() {
        return errorDesc;
    }

    /**
     * 设置errorDesc属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setErrorDesc(String value) {
        this.errorDesc = value;
    }

    /**
     * 获取listOfCrmLoyMemberNew属性的值。
     * 
     * @return
     *     possible object is
     *     {@link ListOfCrmLoyMemberNew }
     *     
     */
    public ListOfCrmLoyMemberNew getListOfCrmLoyMemberNew() {
        return listOfCrmLoyMemberNew;
    }

    /**
     * 设置listOfCrmLoyMemberNew属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link ListOfCrmLoyMemberNew }
     *     
     */
    public void setListOfCrmLoyMemberNew(ListOfCrmLoyMemberNew value) {
        this.listOfCrmLoyMemberNew = value;
    }

}
