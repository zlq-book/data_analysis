
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>getMemberInfoNew complex type的 Java 类。
 * 
 * <p>以下模式片段指定包含在此类中的预期内容。
 * 
 * <pre>
 * &lt;complexType name="getMemberInfoNew">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="memberInfoQueryNewInputEntity" type="{http://service.memberInfoQueryNew.crm_interface.sda.com/}memberInfoQueryNewInputEntity" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "getMemberInfoNew", propOrder = {
    "memberInfoQueryNewInputEntity"
})
public class GetMemberInfoNew {

    protected MemberInfoQueryNewInputEntity memberInfoQueryNewInputEntity;

    /**
     * 获取memberInfoQueryNewInputEntity属性的值。
     * 
     * @return
     *     possible object is
     *     {@link MemberInfoQueryNewInputEntity }
     *     
     */
    public MemberInfoQueryNewInputEntity getMemberInfoQueryNewInputEntity() {
        return memberInfoQueryNewInputEntity;
    }

    /**
     * 设置memberInfoQueryNewInputEntity属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link MemberInfoQueryNewInputEntity }
     *     
     */
    public void setMemberInfoQueryNewInputEntity(MemberInfoQueryNewInputEntity value) {
        this.memberInfoQueryNewInputEntity = value;
    }

}
