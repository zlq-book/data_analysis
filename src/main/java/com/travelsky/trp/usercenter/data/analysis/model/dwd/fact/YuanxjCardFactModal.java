package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.util.Date;

/**
 * 常客-援疆卡-维表
 */
public class YuanxjCardFactModal {

    @JsonProperty("PK_ID")
    private String pkId; // 主键

    @JsonProperty("CRM_CARD")
    private String crmCard; // 凤凰知音卡号

    @JsonProperty("YUANJIANG_CARD")
    private String yuanjiangCard; // 援疆卡卡号

    @JsonProperty("CN_NAME")
    private String cnName; // 姓名

    @JsonProperty("EN_NAME")
    private String enName; // 英文姓名

    @JsonProperty("CERTIFI_TYPE")
    private String certifiType; // 证件类型

    @JsonProperty("CERTIFI_NUMBER")
    private String certifiNumber; // 证件号

    @JsonProperty("CARD_EXPIREDATE")
    private String cardExpiredate; // 有效截止日期

    @JsonProperty("APPLICANTS_TID")
    private String applicantsTid; // 报名人tid

    @JsonProperty("TOTAL_COUNT")
    private Integer totalCount = 1; // 记录计数

    @JsonProperty("SYSTEM_CREATETIME")
    private String systemCreatetime; // 本系统创建日期时间

    @JsonProperty("SYSTEM_LAST_UPDATETIME")
    private String systemLastUpdatetime = LocalDateTime.now().toString(); // 本系统最后更新日期时间

    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getCrmCard() {
        return crmCard;
    }

    public void setCrmCard(String crmCard) {
        this.crmCard = crmCard;
    }

    public String getYuanjiangCard() {
        return yuanjiangCard;
    }

    public void setYuanjiangCard(String yuanjiangCard) {
        this.yuanjiangCard = yuanjiangCard;
    }

    public String getCnName() {
        return cnName;
    }

    public void setCnName(String cnName) {
        this.cnName = cnName;
    }

    public String getEnName() {
        return enName;
    }

    public void setEnName(String enName) {
        this.enName = enName;
    }

    public String getCertifiType() {
        return certifiType;
    }

    public void setCertifiType(String certifiType) {
        this.certifiType = certifiType;
    }

    public String getCertifiNumber() {
        return certifiNumber;
    }

    public void setCertifiNumber(String certifiNumber) {
        this.certifiNumber = certifiNumber;
    }

    public String getCardExpiredate() {
        return cardExpiredate;
    }

    public void setCardExpiredate(String cardExpiredate) {
        this.cardExpiredate = cardExpiredate;
    }

    public String getApplicantsTid() {
        return applicantsTid;
    }

    public void setApplicantsTid(String applicantsTid) {
        this.applicantsTid = applicantsTid;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
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

    @Override
    public String toString() {
        return "YuanxjCardFactModal{" +
                "pkId='" + pkId + '\'' +
                ", crmCard='" + crmCard + '\'' +
                ", yuanjiangCard='" + yuanjiangCard + '\'' +
                ", cnName='" + cnName + '\'' +
                ", enName='" + enName + '\'' +
                ", certifiType='" + certifiType + '\'' +
                ", certifiNumber='" + certifiNumber + '\'' +
                ", cardExpiredate='" + cardExpiredate + '\'' +
                ", applicantsTid='" + applicantsTid + '\'' +
                ", totalCount=" + totalCount +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
