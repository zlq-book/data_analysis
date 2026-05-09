package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;
import java.time.LocalDateTime;


/**
 * 常客-活动报名-用户业务级
 */
public class ActSignupFactModal {

    /**
     * 主键
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 用户报名日期
     */
    @JsonProperty("FK_ACTION_DATE")
    private String fkActionDate;

    /**
     * 活动名称ID
     */
    @JsonProperty("PRODUCT_ID")
    private String productId;

    /**
     * 活动中文名称
     */
    @JsonProperty("PRODUCT_CN_NAME")
    private String productCnName;

    /**
     * 报名次数
     */
    @JsonProperty("ACTION_COUNT")
    private Integer actionCount = 1;

    /**
     * 报名时间
     */
    @JsonProperty("FK_SIGNUP_TIME")
    private String fkSignupTime;

    /**
     * 修改时间
     */
    @JsonProperty("MODIFIED_TIME")
    private String modifiedTime;

    /**
     * 报名人oneid
     */
    @JsonProperty("MEMBER_TID")
    private String memberTid;

    /**
     * 会员卡号
     */
    @JsonProperty("MEMBER_CARD")
    private String memberCard;

    /**
     * 证件号
     */
    @JsonProperty("CERT_NUMBER")
    private String certNumber;

    /**
     * 手机号
     */
    @JsonProperty("MOBILE_NUMBER")
    private String mobileNumber;

    /**
     * 会员级别
     */
    @JsonProperty("MEMBER_LEVEL")
    private String memberLevel;

    /**
     * 中文姓
     */
    @JsonProperty("CN_FIRST_NAME")
    private String cnFirstName;

    /**
     * 中文名
     */
    @JsonProperty("CN_LAST_NAME")
    private String cnLastName;

    /**
     * 英文姓
     */
    @JsonProperty("EN_LAST_NAME")
    private String enLastName;

    /**
     * 英文名
     */
    @JsonProperty("EN_FIRST_NAME")
    private String enFirstName;

    /**
     * 生日
     */
    @JsonProperty("BIRTHDAY")
    private String birthday;

    /**
     * 促销代码
     */
    @JsonProperty("PROMO_CODE")
    private String promoCode;

    /**
     * 报名渠道ID
     */
    @JsonProperty("CHANNEL_ID")
    private String channelId;

    /**
     * 渠道名称
     */
    @JsonProperty("CHANNEL_NAME")
    private String channelName;

    /**
     * 是否达标
     */
    @JsonProperty("IS_QUALIFIED")
    private String isQualified;

    /**
     * 报名类型
     */
    @JsonProperty("SIGNUP_TYPE")
    private String signupType;

    /**
     * 源系统最后更新时间（时间戳）
     */
    @JsonProperty("SOURCE_LAST_UPDATETIME")
    private String sourceLastUpdatetime;

    /**
     * 本系统创建日期时间
     */
    @JsonProperty("SYSTEM_CREATETIME")
    private String systemCreatetime;

    /**
     * 本系统最后更新日期时间
     */
    @JsonProperty("SYSTEM_LAST_UPDATETIME")
    private String systemLastUpdatetime = LocalDateTime.now().toString();

    public ActSignupFactModal() {
    }

    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getFkActionDate() {
        return fkActionDate;
    }

    public void setFkActionDate(String fkActionDate) {
        this.fkActionDate = fkActionDate;
    }

    public Integer getActionCount() {
        return actionCount;
    }

    public void setActionCount(Integer actionCount) {
        this.actionCount = actionCount;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getFkSignupTime() {
        return fkSignupTime;
    }

    public void setFkSignupTime(String fkSignupTime) {
        this.fkSignupTime = fkSignupTime;
    }

    public String getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(String modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    public String getMemberTid() {
        return memberTid;
    }

    public void setMemberTid(String memberTid) {
        this.memberTid = memberTid;
    }

    public String getMemberCard() {
        return memberCard;
    }

    public void setMemberCard(String memberCard) {
        this.memberCard = memberCard;
    }

    public String getCertNumber() {
        return certNumber;
    }

    public void setCertNumber(String certNumber) {
        this.certNumber = certNumber;
    }

    public String getMobileNumber() {
        return mobileNumber;
    }

    public void setMobileNumber(String mobileNumber) {
        this.mobileNumber = mobileNumber;
    }

    public String getMemberLevel() {
        return memberLevel;
    }

    public void setMemberLevel(String memberLevel) {
        this.memberLevel = memberLevel;
    }

    public String getCnFirstName() {
        return cnFirstName;
    }

    public void setCnFirstName(String cnFirstName) {
        this.cnFirstName = cnFirstName;
    }

    public String getCnLastName() {
        return cnLastName;
    }

    public void setCnLastName(String cnLastName) {
        this.cnLastName = cnLastName;
    }

    public String getEnLastName() {
        return enLastName;
    }

    public void setEnLastName(String enLastName) {
        this.enLastName = enLastName;
    }

    public String getEnFirstName() {
        return enFirstName;
    }

    public void setEnFirstName(String enFirstName) {
        this.enFirstName = enFirstName;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public String getPromoCode() {
        return promoCode;
    }

    public void setPromoCode(String promoCode) {
        this.promoCode = promoCode;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public String getSourceLastUpdatetime() {
        return sourceLastUpdatetime;
    }

    public void setSourceLastUpdatetime(String sourceLastUpdatetime) {
        this.sourceLastUpdatetime = sourceLastUpdatetime;
    }

    public String getSystemCreatetime() {
        return systemCreatetime;
    }

    public void setSystemCreatetime(String systemCreatetime) {
        this.systemCreatetime = systemCreatetime;
    }

    public String getSystemLastUpdatetime() {
        return systemLastUpdatetime;
    }

    public void setSystemLastUpdatetime(String systemLastUpdatetime) {
        this.systemLastUpdatetime = systemLastUpdatetime;
    }

    public String getProductCnName() {
        return productCnName;
    }

    public void setProductCnName(String productCnName) {
        this.productCnName = productCnName;
    }

    public String getSignupType() {
        return signupType;
    }

    public void setSignupType(String signupType) {
        this.signupType = signupType;
    }

    public String getIsQualified() {
        return isQualified;
    }

    public void setIsQualified(String isQualified) {
        this.isQualified = isQualified;
    }

    @Override
    public String toString() {
        return "ActSignupFactModal{" +
                "pkId='" + pkId + '\'' +
                ", fkActionDate='" + fkActionDate + '\'' +
                ", productId='" + productId + '\'' +
                ", productCnName='" + productCnName + '\'' +
                ", actionCount=" + actionCount +
                ", fkSignupTime='" + fkSignupTime + '\'' +
                ", modifiedTime='" + modifiedTime + '\'' +
                ", memberTid='" + memberTid + '\'' +
                ", memberCard='" + memberCard + '\'' +
                ", certNumber='" + certNumber + '\'' +
                ", mobileNumber='" + mobileNumber + '\'' +
                ", memberLevel='" + memberLevel + '\'' +
                ", cnFirstName='" + cnFirstName + '\'' +
                ", cnLastName='" + cnLastName + '\'' +
                ", enLastName='" + enLastName + '\'' +
                ", enFirstName='" + enFirstName + '\'' +
                ", birthday='" + birthday + '\'' +
                ", promoCode='" + promoCode + '\'' +
                ", channelId='" + channelId + '\'' +
                ", channelName='" + channelName + '\'' +
                ", isQualified='" + isQualified + '\'' +
                ", signupType='" + signupType + '\'' +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
