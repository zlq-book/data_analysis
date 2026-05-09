
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.querymemberext;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>additionInfo complex type的 Java 类。
 * 
 * <p>以下模式片段指定包含在此类中的预期内容。
 * 
 * <pre>
 * &lt;complexType name="additionInfo">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="addressList" type="{http://service.QueryMemberExt.crm_interface.sda.com/}addressList" minOccurs="0"/>
 *         &lt;element name="autoIdentFlg" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="emailList" type="{http://service.QueryMemberExt.crm_interface.sda.com/}emailList" minOccurs="0"/>
 *         &lt;element name="language" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="noProEmailFlag" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="noProSMSFlag" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="phoneList" type="{http://service.QueryMemberExt.crm_interface.sda.com/}phoneList" minOccurs="0"/>
 *         &lt;element name="primaryAddressId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="primaryEmailId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="primaryPhoneId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="unAcceptAPPMessage" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="unAcceptSMSMessage" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="unAcceptWXMessage" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "additionInfo", propOrder = {
    "addressList",
    "autoIdentFlg",
    "emailList",
    "language",
    "noProEmailFlag",
    "noProSMSFlag",
    "phoneList",
    "primaryAddressId",
    "primaryEmailId",
    "primaryPhoneId",
    "unAcceptAPPMessage",
    "unAcceptSMSMessage",
    "unAcceptWXMessage"
})
public class AdditionInfo {

    protected AddressList addressList;
    protected String autoIdentFlg;
    protected EmailList emailList;
    protected String language;
    protected String noProEmailFlag;
    protected String noProSMSFlag;
    protected PhoneList phoneList;
    protected String primaryAddressId;
    protected String primaryEmailId;
    protected String primaryPhoneId;
    protected String unAcceptAPPMessage;
    protected String unAcceptSMSMessage;
    protected String unAcceptWXMessage;

    /**
     * 获取addressList属性的值。
     * 
     * @return
     *     possible object is
     *     {@link AddressList }
     *     
     */
    public AddressList getAddressList() {
        return addressList;
    }

    /**
     * 设置addressList属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link AddressList }
     *     
     */
    public void setAddressList(AddressList value) {
        this.addressList = value;
    }

    /**
     * 获取autoIdentFlg属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getAutoIdentFlg() {
        return autoIdentFlg;
    }

    /**
     * 设置autoIdentFlg属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setAutoIdentFlg(String value) {
        this.autoIdentFlg = value;
    }

    /**
     * 获取emailList属性的值。
     * 
     * @return
     *     possible object is
     *     {@link EmailList }
     *     
     */
    public EmailList getEmailList() {
        return emailList;
    }

    /**
     * 设置emailList属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link EmailList }
     *     
     */
    public void setEmailList(EmailList value) {
        this.emailList = value;
    }

    /**
     * 获取language属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getLanguage() {
        return language;
    }

    /**
     * 设置language属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setLanguage(String value) {
        this.language = value;
    }

    /**
     * 获取noProEmailFlag属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getNoProEmailFlag() {
        return noProEmailFlag;
    }

    /**
     * 设置noProEmailFlag属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setNoProEmailFlag(String value) {
        this.noProEmailFlag = value;
    }

    /**
     * 获取noProSMSFlag属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getNoProSMSFlag() {
        return noProSMSFlag;
    }

    /**
     * 设置noProSMSFlag属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setNoProSMSFlag(String value) {
        this.noProSMSFlag = value;
    }

    /**
     * 获取phoneList属性的值。
     * 
     * @return
     *     possible object is
     *     {@link PhoneList }
     *     
     */
    public PhoneList getPhoneList() {
        return phoneList;
    }

    /**
     * 设置phoneList属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link PhoneList }
     *     
     */
    public void setPhoneList(PhoneList value) {
        this.phoneList = value;
    }

    /**
     * 获取primaryAddressId属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPrimaryAddressId() {
        return primaryAddressId;
    }

    /**
     * 设置primaryAddressId属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPrimaryAddressId(String value) {
        this.primaryAddressId = value;
    }

    /**
     * 获取primaryEmailId属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPrimaryEmailId() {
        return primaryEmailId;
    }

    /**
     * 设置primaryEmailId属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPrimaryEmailId(String value) {
        this.primaryEmailId = value;
    }

    /**
     * 获取primaryPhoneId属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPrimaryPhoneId() {
        return primaryPhoneId;
    }

    /**
     * 设置primaryPhoneId属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPrimaryPhoneId(String value) {
        this.primaryPhoneId = value;
    }

    /**
     * 获取unAcceptAPPMessage属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUnAcceptAPPMessage() {
        return unAcceptAPPMessage;
    }

    /**
     * 设置unAcceptAPPMessage属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUnAcceptAPPMessage(String value) {
        this.unAcceptAPPMessage = value;
    }

    /**
     * 获取unAcceptSMSMessage属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUnAcceptSMSMessage() {
        return unAcceptSMSMessage;
    }

    /**
     * 设置unAcceptSMSMessage属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUnAcceptSMSMessage(String value) {
        this.unAcceptSMSMessage = value;
    }

    /**
     * 获取unAcceptWXMessage属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUnAcceptWXMessage() {
        return unAcceptWXMessage;
    }

    /**
     * 设置unAcceptWXMessage属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUnAcceptWXMessage(String value) {
        this.unAcceptWXMessage = value;
    }

}
