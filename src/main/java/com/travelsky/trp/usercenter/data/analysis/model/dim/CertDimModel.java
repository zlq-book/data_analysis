package com.travelsky.trp.usercenter.data.analysis.model.dim;

import java.util.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 证件信息维表
 */
public class CertDimModel {

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
     * 证件类型
     */
    @JsonProperty("CERT_TYPE")
    private String certType;

    /**
     * 证件号码
     */
    @JsonProperty("CERT_NUMBER")
    private String certNumber;

    /**
     * 是否直销实名认证证件
     */
    @JsonProperty("IS_DIRECT_SALES_REAL_NAME_VERIFIED_CARD")
    private Boolean isDirectSalesRealNameVerifiedCard;

    /**
     * 是否鲁雁行实名认证证件
     */
    @JsonProperty("IS_LY_REGIST_CARD")
    private Boolean isLyRegistCard;

    /**
     * 是否常客注册证件证件
     */
    @JsonProperty("IS_FREQUENT_FLYER_REGISTRATION_CARD")
    private Boolean isFrequentFlyerRegistrationCard;

    public Boolean getCorpTravelMilitaryCard() {
        return isCorpTravelMilitaryCard;
    }

    public void setCorpTravelMilitaryCard(Boolean corpTravelMilitaryCard) {
        isCorpTravelMilitaryCard = corpTravelMilitaryCard;
    }

    /**
     * 是否次卡受益人证件
     */
    @JsonProperty("IS_PURCHASERS_BENEFICIARY_CARD")
    private Boolean isPurchasersBeneficiaryCard;

    /**
     * 是否抖音次卡受益人证件
     */
    @JsonProperty("IS_DOUYIN_PURCHASERS_BENEFICIARY_CARD")
    private Boolean isDouyinPurchasersBeneficiaryCard;

    /**
     * 是否大客户差旅用户
     */
    @JsonProperty("IS_CORP_TRAVEL_MILITARY_CARD")
    private Boolean isCorpTravelMilitaryCard;


    /**
     * 是否符合编码规则
     */
    @JsonProperty("IS_CODE_RULE_COMPLIANT")
    private Boolean isCodeRuleCompliant;

    /**
     * 证件签发日期
     */
    @JsonProperty("CERT_ISSUE_DATE")
    private LocalDate certIssueDate;

    /**
     * 证件过期日期
     */
    @JsonProperty("CERT_EXPIRE_DATE")
    private LocalDate certExpireDate;

    /**
     * 证件签发国
     */
    @JsonProperty("CERT_ISSUING_COUNTRY")
    private String certIssuingCountry;

    /**
     * 证件签发机构
     */
    @JsonProperty("CERT_ISSUING_AUTHORITY")
    private String certIssuingAuthority;

    /**
     * 当前证件最高优先级
     */
    @JsonProperty("CURRENT_CERT_HIGHEST_PRIORITY")
    private Integer currentCertHighestPriority;

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

    public String getCertType() {
        return certType;
    }

    public void setCertType(String certType) {
        this.certType = certType;
    }

    public String getCertNumber() {
        return certNumber;
    }

    public void setCertNumber(String certNumber) {
        this.certNumber = certNumber;
    }

    public Boolean getDirectSalesRealNameVerifiedCard() {
        return isDirectSalesRealNameVerifiedCard;
    }

    public void setDirectSalesRealNameVerifiedCard(Boolean directSalesRealNameVerifiedCard) {
        isDirectSalesRealNameVerifiedCard = directSalesRealNameVerifiedCard;
    }

    public Boolean getLyRegistCard() {
        return isLyRegistCard;
    }

    public void setLyRegistCard(Boolean lyRegistCard) {
        isLyRegistCard = lyRegistCard;
    }

    public Boolean getFrequentFlyerRegistrationCard() {
        return isFrequentFlyerRegistrationCard;
    }

    public void setFrequentFlyerRegistrationCard(Boolean frequentFlyerRegistrationCard) {
        isFrequentFlyerRegistrationCard = frequentFlyerRegistrationCard;
    }

    public Boolean getPurchasersBeneficiaryCard() {
        return isPurchasersBeneficiaryCard;
    }

    public void setPurchasersBeneficiaryCard(Boolean purchasersBeneficiaryCard) {
        isPurchasersBeneficiaryCard = purchasersBeneficiaryCard;
    }

    public Boolean getDouyinPurchasersBeneficiaryCard() {
        return isDouyinPurchasersBeneficiaryCard;
    }

    public void setDouyinPurchasersBeneficiaryCard(Boolean douyinPurchasersBeneficiaryCard) {
        isDouyinPurchasersBeneficiaryCard = douyinPurchasersBeneficiaryCard;
    }

    public Boolean getCodeRuleCompliant() {
        return isCodeRuleCompliant;
    }

    public void setCodeRuleCompliant(Boolean codeRuleCompliant) {
        isCodeRuleCompliant = codeRuleCompliant;
    }

    public LocalDate getCertIssueDate() {
        return certIssueDate;
    }

    public void setCertIssueDate(LocalDate certIssueDate) {
        this.certIssueDate = certIssueDate;
    }

    public LocalDate getCertExpireDate() {
        return certExpireDate;
    }

    public void setCertExpireDate(LocalDate certExpireDate) {
        this.certExpireDate = certExpireDate;
    }

    public String getCertIssuingCountry() {
        return certIssuingCountry;
    }

    public void setCertIssuingCountry(String certIssuingCountry) {
        this.certIssuingCountry = certIssuingCountry;
    }

    public String getCertIssuingAuthority() {
        return certIssuingAuthority;
    }

    public void setCertIssuingAuthority(String certIssuingAuthority) {
        this.certIssuingAuthority = certIssuingAuthority;
    }

    public Integer getCurrentCertHighestPriority() {
        return currentCertHighestPriority;
    }

    public void setCurrentCertHighestPriority(Integer currentCertHighestPriority) {
        this.currentCertHighestPriority = currentCertHighestPriority;
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
        return "CertDimModel{" +
                "systemKey='" + systemKey + '\'' +
                ", tId='" + tId + '\'' +
                ", certType='" + certType + '\'' +
                ", certNumber='" + certNumber + '\'' +
                ", isDirectSalesRealNameVerifiedCard=" + isDirectSalesRealNameVerifiedCard +
                ", isLyRegistCard=" + isLyRegistCard +
                ", isFrequentFlyerRegistrationCard=" + isFrequentFlyerRegistrationCard +
                ", isPurchasersBeneficiaryCard=" + isPurchasersBeneficiaryCard +
                ", isDouyinPurchasersBeneficiaryCard=" + isDouyinPurchasersBeneficiaryCard +
                ", isCodeRuleCompliant=" + isCodeRuleCompliant +
                ", certIssueDate=" + certIssueDate +
                ", certExpireDate=" + certExpireDate +
                ", certIssuingCountry='" + certIssuingCountry + '\'' +
                ", certIssuingAuthority='" + certIssuingAuthority + '\'' +
                ", currentCertHighestPriority=" + currentCertHighestPriority +
                ", updateTime='" + updateTime + '\'' +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}