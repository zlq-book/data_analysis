package com.travelsky.trp.usercenter.data.analysis.model.dim;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 手机号维表
 */
public class MobileDimModel {

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
     * 手机号
     */
    @JsonProperty("MOBILE_NUMBER")
    private String mobileNumber;

    /**
     * 是否直销实名认证手机号
     */
    @JsonProperty("IS_DIRECT_SALE_MOBILE")
    private Boolean isDirectSaleMobile;

    /**
     * 是否鲁雁行实名认证用户手机号
     */
    @JsonProperty("IS_LYX_REALNAME_MOBILE")
    private Boolean isLyxRealnameMobile;

    /**
     * 是否为抖音次卡购买人手机号
     */
    @JsonProperty("IS_DOUYIN_CARD_PURCHASERS_MOBILE")
    private Boolean isDouyinCardPurchasersMobile;

    /**
     * 是否为常客手机号
     */
    @JsonProperty("IS_FREQUENT_FLYER_MOBILE")
    private Boolean isFrequentFlyerMobile;

    /**
     * 是否高端旅客手机号
     */
    @JsonProperty("IS_HIGH_MOBILE_NUMBER")
    private Boolean isHighMobileNumber;

    /**
     * 当前手机号最高优先级
     */
    @JsonProperty("CURRENT_PHONE_HIGHEST_PRIORITY")
    private Integer currentPhoneHighestPriority;

    /**
     * 是否虚拟手机号
     */
    @JsonProperty("IS_VIRTUAL_MOBILE")
    private Boolean isVirtualMobile;

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

    public String getMobileNumber() {
        return mobileNumber;
    }

    public void setMobileNumber(String mobileNumber) {
        this.mobileNumber = mobileNumber;
    }

    public Boolean getDirectSaleMobile() {
        return isDirectSaleMobile;
    }

    public void setDirectSaleMobile(Boolean directSaleMobile) {
        isDirectSaleMobile = directSaleMobile;
    }

    public Boolean getLyxRealnameMobile() {
        return isLyxRealnameMobile;
    }

    public void setLyxRealnameMobile(Boolean lyxRealnameMobile) {
        isLyxRealnameMobile = lyxRealnameMobile;
    }

    public Boolean getDouyinCardPurchasersMobile() {
        return isDouyinCardPurchasersMobile;
    }

    public void setDouyinCardPurchasersMobile(Boolean douyinCardPurchasersMobile) {
        isDouyinCardPurchasersMobile = douyinCardPurchasersMobile;
    }

    public Boolean getFrequentFlyerMobile() {
        return isFrequentFlyerMobile;
    }

    public void setFrequentFlyerMobile(Boolean frequentFlyerMobile) {
        isFrequentFlyerMobile = frequentFlyerMobile;
    }

    public Boolean getHighMobileNumber() {
        return isHighMobileNumber;
    }

    public void setHighMobileNumber(Boolean highMobileNumber) {
        isHighMobileNumber = highMobileNumber;
    }

    public Integer getCurrentPhoneHighestPriority() {
        return currentPhoneHighestPriority;
    }

    public void setCurrentPhoneHighestPriority(Integer currentPhoneHighestPriority) {
        this.currentPhoneHighestPriority = currentPhoneHighestPriority;
    }

    public Boolean getVirtualMobile() {
        return isVirtualMobile;
    }

    public void setVirtualMobile(Boolean virtualMobile) {
        isVirtualMobile = virtualMobile;
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
        return "MobileDimModel{" +
                "systemKey='" + systemKey + '\'' +
                ", tId='" + tId + '\'' +
                ", mobileNumber='" + mobileNumber + '\'' +
                ", isDirectSaleMobile=" + isDirectSaleMobile +
                ", isLyxRealnameMobile=" + isLyxRealnameMobile +
                ", isDouyinCardPurchasersMobile=" + isDouyinCardPurchasersMobile +
                ", isFrequentFlyerMobile=" + isFrequentFlyerMobile +
                ", isHighMobileNumber=" + isHighMobileNumber +
                ", currentPhoneHighestPriority=" + currentPhoneHighestPriority +
                ", isVirtualMobile=" + isVirtualMobile +
                ", updateTime='" + updateTime + '\'' +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}