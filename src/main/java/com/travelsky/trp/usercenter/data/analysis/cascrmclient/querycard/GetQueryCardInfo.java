
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.querycard;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>getQueryCardInfo complex type的 Java 类。
 * 
 * <p>以下模式片段指定包含在此类中的预期内容。
 * 
 * <pre>
 * &lt;complexType name="getQueryCardInfo">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="queryCardInputEntity" type="{http://service.QueryCard.crm_interface.sda.com/}queryCardInputEntity" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "getQueryCardInfo", propOrder = {
    "queryCardInputEntity"
})
public class GetQueryCardInfo {

    protected QueryCardInputEntity queryCardInputEntity;

    /**
     * 获取queryCardInputEntity属性的值。
     * 
     * @return
     *     possible object is
     *     {@link QueryCardInputEntity }
     *     
     */
    public QueryCardInputEntity getQueryCardInputEntity() {
        return queryCardInputEntity;
    }

    /**
     * 设置queryCardInputEntity属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link QueryCardInputEntity }
     *     
     */
    public void setQueryCardInputEntity(QueryCardInputEntity value) {
        this.queryCardInputEntity = value;
    }

}
