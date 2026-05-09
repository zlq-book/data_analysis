
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>listOfCrmLoyMemberNew complex type的 Java 类。
 * 
 * <p>以下模式片段指定包含在此类中的预期内容。
 * 
 * <pre>
 * &lt;complexType name="listOfCrmLoyMemberNew">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="memberInfo" type="{http://service.memberInfoQueryNew.crm_interface.sda.com/}memberInfo" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "listOfCrmLoyMemberNew", propOrder = {
    "memberInfo"
})
public class ListOfCrmLoyMemberNew {

    protected MemberInfo memberInfo;

    /**
     * 获取memberInfo属性的值。
     * 
     * @return
     *     possible object is
     *     {@link MemberInfo }
     *     
     */
    public MemberInfo getMemberInfo() {
        return memberInfo;
    }

    /**
     * 设置memberInfo属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link MemberInfo }
     *     
     */
    public void setMemberInfo(MemberInfo value) {
        this.memberInfo = value;
    }

}
