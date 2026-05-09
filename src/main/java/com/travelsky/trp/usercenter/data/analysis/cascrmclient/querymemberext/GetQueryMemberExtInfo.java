
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.querymemberext;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>getQueryMemberExtInfo complex type的 Java 类。
 * 
 * <p>以下模式片段指定包含在此类中的预期内容。
 * 
 * <pre>
 * &lt;complexType name="getQueryMemberExtInfo">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="queryMemberExtInputEntity" type="{http://service.QueryMemberExt.crm_interface.sda.com/}queryMemberExtInputEntity" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "getQueryMemberExtInfo", propOrder = {
    "queryMemberExtInputEntity"
})
public class GetQueryMemberExtInfo {

    protected QueryMemberExtInputEntity queryMemberExtInputEntity;

    /**
     * 获取queryMemberExtInputEntity属性的值。
     * 
     * @return
     *     possible object is
     *     {@link QueryMemberExtInputEntity }
     *     
     */
    public QueryMemberExtInputEntity getQueryMemberExtInputEntity() {
        return queryMemberExtInputEntity;
    }

    /**
     * 设置queryMemberExtInputEntity属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link QueryMemberExtInputEntity }
     *     
     */
    public void setQueryMemberExtInputEntity(QueryMemberExtInputEntity value) {
        this.queryMemberExtInputEntity = value;
    }

}
