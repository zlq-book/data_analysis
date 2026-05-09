
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>getMemberInfoNewResponse complex type的 Java 类。
 * 
 * <p>以下模式片段指定包含在此类中的预期内容。
 * 
 * <pre>
 * &lt;complexType name="getMemberInfoNewResponse">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="return" type="{http://service.memberInfoQueryNew.crm_interface.sda.com/}memberInfoQueryNewOutputEntity" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "getMemberInfoNewResponse", propOrder = {
    "_return"
})
public class GetMemberInfoNewResponse {

    @XmlElement(name = "return")
    protected MemberInfoQueryNewOutputEntity _return;

    /**
     * 获取return属性的值。
     * 
     * @return
     *     possible object is
     *     {@link MemberInfoQueryNewOutputEntity }
     *     
     */
    public MemberInfoQueryNewOutputEntity getReturn() {
        return _return;
    }

    /**
     * 设置return属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link MemberInfoQueryNewOutputEntity }
     *     
     */
    public void setReturn(MemberInfoQueryNewOutputEntity value) {
        this._return = value;
    }

}
