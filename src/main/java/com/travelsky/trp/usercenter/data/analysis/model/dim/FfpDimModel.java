package com.travelsky.trp.usercenter.data.analysis.model.dim;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 常客信息维表
 */
public class FfpDimModel {

    /**
     * 主键
     */
    @JsonProperty("SYSTEM_KEY")
    private String systemKey;

    /**
     * 用户ID
     */
    @JsonProperty("T_ID")
    private String tId;

    /**
     * 常客卡号
     */
    @JsonProperty("FFRF")
    private String ffrf;

    /**
     * 常客等级
     */
    @JsonProperty("FF_LEVEL")
    private String ffLevel;

    /**
     * 是否父母常客卡号
     */
    @JsonProperty("PARENT_CARD_NUMBER")
    private String parentCardNumber;

    /**
     * 注册常旅客时间
     */
    @JsonProperty("FT_REGISTER_TIME")
    private String ftRegisterTime;

    /**
     * 援疆卡卡号
     */
    @JsonProperty("YJ_CARD")
    private String yjCard;

    /**
     * 援疆卡有效期
     */
    @JsonProperty("YJ_CARD_EXPIREDATE")
    private String yjCardExpiredate;

    /**
     * 常旅客发展渠道（一级）
     */
    @JsonProperty("DEV_CHANNEL_ONE")
    private String devChannelOne;

    /**
     * 常旅客发展渠道（二级）
     */
    @JsonProperty("DEV_CHANNEL_TWO")
    private String devChannelTwo;

    /**
     * 常旅客发展渠道（三级）
     */
    @JsonProperty("DEV_CHANNEL_THREE")
    private String devChannelThree;

    /**
     * 常旅客发展渠道（四级）
     */
    @JsonProperty("DEV_CHANNEL_FOUR")
    private String devChannelFour;

    /**
     * 常客证件类型
     */
    @JsonProperty("FF_CERT_TYPE")
    private String ffCertType;

    /**
     * 常客证件号码
     */
    @JsonProperty("FF_CERT_NUMBER")
    private String ffCertNumber;

    /**
     * 当前最可信来源
     */
    @JsonProperty("MOST_TRUSTED_SOURCE")
    private String mostTrustedSource;

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

    public String getFfrf() {
        return ffrf;
    }

    public void setFfrf(String ffrf) {
        this.ffrf = ffrf;
    }

    public String getFfLevel() {
        return ffLevel;
    }

    public void setFfLevel(String ffLevel) {
        this.ffLevel = ffLevel;
    }

    public String getParentCardNumber() {
        return parentCardNumber;
    }

    public void setParentCardNumber(String parentCardNumber) {
        this.parentCardNumber = parentCardNumber;
    }

    public String getFtRegisterTime() {
        return ftRegisterTime;
    }

    public void setFtRegisterTime(String ftRegisterTime) {
        this.ftRegisterTime = ftRegisterTime;
    }

    public String getYjCard() {
        return yjCard;
    }

    public void setYjCard(String yjCard) {
        this.yjCard = yjCard;
    }

    public String getYjCardExpiredate() {
        return yjCardExpiredate;
    }

    public void setYjCardExpiredate(String yjCardExpiredate) {
        this.yjCardExpiredate = yjCardExpiredate;
    }

    public String getDevChannelOne() {
        return devChannelOne;
    }

    public void setDevChannelOne(String devChannelOne) {
        this.devChannelOne = devChannelOne;
    }

    public String getDevChannelTwo() {
        return devChannelTwo;
    }

    public void setDevChannelTwo(String devChannelTwo) {
        this.devChannelTwo = devChannelTwo;
    }

    public String getDevChannelThree() {
        return devChannelThree;
    }

    public void setDevChannelThree(String devChannelThree) {
        this.devChannelThree = devChannelThree;
    }

    public String getDevChannelFour() {
        return devChannelFour;
    }

    public void setDevChannelFour(String devChannelFour) {
        this.devChannelFour = devChannelFour;
    }

    public String getFfCertType() {
        return ffCertType;
    }

    public void setFfCertType(String ffCertType) {
        this.ffCertType = ffCertType;
    }

    public String getFfCertNumber() {
        return ffCertNumber;
    }

    public void setFfCertNumber(String ffCertNumber) {
        this.ffCertNumber = ffCertNumber;
    }

    public String getMostTrustedSource() {
        return mostTrustedSource;
    }

    public void setMostTrustedSource(String mostTrustedSource) {
        this.mostTrustedSource = mostTrustedSource;
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
        return "FfpDimModel{" +
                "systemKey='" + systemKey + '\'' +
                ", tId='" + tId + '\'' +
                ", ffrf='" + ffrf + '\'' +
                ", ffLevel='" + ffLevel + '\'' +
                ", parentCardNumber=" + parentCardNumber +
                ", ftRegisterTime='" + ftRegisterTime + '\'' +
                ", yjCard='" + yjCard + '\'' +
                ", yjCardExpiredate='" + yjCardExpiredate + '\'' +
                ", devChannelOne='" + devChannelOne + '\'' +
                ", devChannelTwo='" + devChannelTwo + '\'' +
                ", devChannelThree='" + devChannelThree + '\'' +
                ", devChannelFour='" + devChannelFour + '\'' +
                ", ffCertType='" + ffCertType + '\'' +
                ", ffCertNumber='" + ffCertNumber + '\'' +
                ", mostTrustedSource='" + mostTrustedSource + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}
