
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.querymemberext;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>listOfCrmLoyMemberMoreInfo complex type的 Java 类。
 * 
 * <p>以下模式片段指定包含在此类中的预期内容。
 * 
 * <pre>
 * &lt;complexType name="listOfCrmLoyMemberMoreInfo">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="additionInfo" type="{http://service.QueryMemberExt.crm_interface.sda.com/}additionInfo" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "listOfCrmLoyMemberMoreInfo", propOrder = {
    "additionInfo"
})
public class ListOfCrmLoyMemberMoreInfo {

    protected AdditionInfo additionInfo;

    /**
     * 获取additionInfo属性的值。
     * 
     * @return
     *     possible object is
     *     {@link AdditionInfo }
     *     
     */
    public AdditionInfo getAdditionInfo() {
        return additionInfo;
    }

    /**
     * 设置additionInfo属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link AdditionInfo }
     *     
     */
    public void setAdditionInfo(AdditionInfo value) {
        this.additionInfo = value;
    }

}
