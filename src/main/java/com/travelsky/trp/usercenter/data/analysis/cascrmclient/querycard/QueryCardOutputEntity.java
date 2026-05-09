
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.querycard;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>queryCardOutputEntity complex type的 Java 类。
 * 
 * <p>以下模式片段指定包含在此类中的预期内容。
 * 
 * <pre>
 * &lt;complexType name="queryCardOutputEntity">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="errorCode" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="errorDesc" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="memberInfoList" type="{http://service.QueryCard.crm_interface.sda.com/}memberInfoList" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "queryCardOutputEntity", propOrder = {
    "errorCode",
    "errorDesc",
    "memberInfoList"
})
public class QueryCardOutputEntity {

    protected String errorCode;
    protected String errorDesc;
    protected MemberInfoList memberInfoList;

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
     * 获取memberInfoList属性的值。
     * 
     * @return
     *     possible object is
     *     {@link MemberInfoList }
     *     
     */
    public MemberInfoList getMemberInfoList() {
        return memberInfoList;
    }

    /**
     * 设置memberInfoList属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link MemberInfoList }
     *     
     */
    public void setMemberInfoList(MemberInfoList value) {
        this.memberInfoList = value;
    }

}
