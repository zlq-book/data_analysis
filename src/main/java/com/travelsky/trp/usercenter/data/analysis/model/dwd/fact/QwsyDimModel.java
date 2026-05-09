package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 企微小山客户信息维表模型
 */
public class QwsyDimModel {

    /**
     * 主键
     */
    @JsonProperty("SYSTEM_KEY")
    private String systemKey;

    /**
     * 用户T_ID
     */
    @JsonProperty("T_ID")
    private String tId;

    /**
     * 企业id
     */
    @JsonProperty("COMPANY_ID")
    private String companyId;

    /**
     * 企微客户id
     */
    @JsonProperty("QW_CUSTOMER_ID")
    private String qwCustomerId;

    /**
     * 企微客户等级
     */
    @JsonProperty("QW_CUSTOMER_LEVEL")
    private String qwCustomerLevel;

    /**
     * 企微客户个人标签
     */
    @JsonProperty("QW_CUSTOMER_TAGS")
    private String qwCustomerTags;

    /**
     * 更新时间
     */
    @JsonProperty("UPDATE_TIME")
    private String updateTime;

    /**
     * 创建时间
     */
    @JsonProperty("CREATE_TIME")
    private String createTime;

    // Getter 和 Setter 方法

    public String getSystemKey() {
        return systemKey;
    }

    public void setSystemKey(String systemKey) {
        this.systemKey = systemKey;
    }

    public String gettId() {
        return tId;
    }

    public void settId(String tId) {
        this.tId = tId;
    }

    public String getCompanyId() {
        return companyId;
    }

    public void setCompanyId(String companyId) {
        this.companyId = companyId;
    }

    public String getQwCustomerId() {
        return qwCustomerId;
    }

    public void setQwCustomerId(String qwCustomerId) {
        this.qwCustomerId = qwCustomerId;
    }

    public String getQwCustomerLevel() {
        return qwCustomerLevel;
    }

    public void setQwCustomerLevel(String qwCustomerLevel) {
        this.qwCustomerLevel = qwCustomerLevel;
    }

    public String getQwCustomerTags() {
        return qwCustomerTags;
    }

    public void setQwCustomerTags(String qwCustomerTags) {
        this.qwCustomerTags = qwCustomerTags;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "QwsyDimModel{" +
                "systemKey='" + systemKey + '\'' +
                ", tId='" + tId + '\'' +
                ", companyId='" + companyId + '\'' +
                ", qwCustomerId='" + qwCustomerId + '\'' +
                ", qwCustomerLevel='" + qwCustomerLevel + '\'' +
                ", qwCustomerTags='" + qwCustomerTags + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}