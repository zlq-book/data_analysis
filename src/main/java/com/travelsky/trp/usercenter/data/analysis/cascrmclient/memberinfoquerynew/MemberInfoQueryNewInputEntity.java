
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>memberInfoQueryNewInputEntity complex type的 Java 类。
 * 
 * <p>以下模式片段指定包含在此类中的预期内容。
 * 
 * <pre>
 * &lt;complexType name="memberInfoQueryNewInputEntity">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="CRMMemberId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="channel" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="memberNumber" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="SC_channel" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="SC_sub_channel" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="subChannel" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="tradeId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "memberInfoQueryNewInputEntity", propOrder = {
    "crmMemberId",
    "channel",
    "memberNumber",
    "scChannel",
    "scSubChannel",
    "subChannel",
    "tradeId"
})
public class MemberInfoQueryNewInputEntity {

    @XmlElement(name = "CRMMemberId")
    protected String crmMemberId;
    protected String channel;
    protected String memberNumber;
    @XmlElement(name = "SC_channel")
    protected String scChannel;
    @XmlElement(name = "SC_sub_channel")
    protected String scSubChannel;
    protected String subChannel;
    protected String tradeId;

    /**
     * 获取crmMemberId属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCRMMemberId() {
        return crmMemberId;
    }

    /**
     * 设置crmMemberId属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCRMMemberId(String value) {
        this.crmMemberId = value;
    }

    /**
     * 获取channel属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getChannel() {
        return channel;
    }

    /**
     * 设置channel属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setChannel(String value) {
        this.channel = value;
    }

    /**
     * 获取memberNumber属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getMemberNumber() {
        return memberNumber;
    }

    /**
     * 设置memberNumber属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setMemberNumber(String value) {
        this.memberNumber = value;
    }

    /**
     * 获取scChannel属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSCChannel() {
        return scChannel;
    }

    /**
     * 设置scChannel属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSCChannel(String value) {
        this.scChannel = value;
    }

    /**
     * 获取scSubChannel属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSCSubChannel() {
        return scSubChannel;
    }

    /**
     * 设置scSubChannel属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSCSubChannel(String value) {
        this.scSubChannel = value;
    }

    /**
     * 获取subChannel属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSubChannel() {
        return subChannel;
    }

    /**
     * 设置subChannel属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSubChannel(String value) {
        this.subChannel = value;
    }

    /**
     * 获取tradeId属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTradeId() {
        return tradeId;
    }

    /**
     * 设置tradeId属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTradeId(String value) {
        this.tradeId = value;
    }

}
