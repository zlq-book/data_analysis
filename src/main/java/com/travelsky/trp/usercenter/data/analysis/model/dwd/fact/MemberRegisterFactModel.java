package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

public class MemberRegisterFactModel {
    @JsonProperty("PK_ID")
    private String pkId; // 常客会员卡号

    @JsonProperty("FK_TID")
    private String fkTid; // T_ID

    @JsonProperty("FK_REGISTER_TIME")
    private String fkRegisterTime; // 注册时间

    @JsonProperty("FK_REGISTER_DATE")
    private String fkRegisterDate; // 注册日期

    @JsonProperty("FREQUENT_CARD")
    private String frequentCard; // 常客卡号

    @JsonProperty("TEST_MEMBER")
    private Boolean testMember; // 是否为测试会员

    @JsonProperty("GENDER")
    private String gender; // 性别

    @JsonProperty("FIRST_EXCH_QUALIFI")
    private String firstExchQualifi; // 航空首兑资格

    @JsonProperty("CERT_NUMBER")
    private String certNumber; // 证件号

    @JsonProperty("CERT_TYPE")
    private String certType; // 证件类型

    @JsonProperty("EXPIRE_TIME")
    private String expireTime; // 证件过期时间

    @JsonProperty("ISSUE_TIME")
    private String issueTime; // 证件发证时间

    @JsonProperty("ISSUE_COUNTRY")
    private String issueCountry; // 证件发证国家

    @JsonProperty("EN_FIRST_NAME")
    private String enFirstName; // 英文名

    @JsonProperty("EN_LAST_NAME")
    private String enLastName; // 英文姓

    @JsonProperty("CN_NAME")
    private String cnName; // 中文名字

    @JsonProperty("CH_LAST_NAME")
    private String chLastName; // 中文姓氏

    @JsonProperty("BIRTHDAY")
    private String birthday; // 生日

    @JsonProperty("NATIONALITY")
    private String nationality; // 国籍

    @JsonProperty("CONTACT_LANGUAGE")
    private String contactLanguage; // 联系语言

    @JsonProperty("PARENT_CARD_NUMBER")
    private String parentCardNumber; // 父母常客卡号

    @JsonProperty("DEV_STAFF_ID")
    private String devStaffId; // 发展人工号

    @JsonProperty("DEV_NAME")
    private String devName; // 发展人姓名

    @JsonProperty("DEV_DEPARTMENT")
    private String devDepartment; // 发展人所属部门

    @JsonProperty("DEV_CHANNEL_ONE")
    private String devChannelOne; // 一级发展渠道ID

    @JsonProperty("DEV_CHANNEL_TWO")
    private String devChannelTow; // 二级发展渠道ID

    @JsonProperty("DEV_CHANNEL_THREE")
    private String devChannelThree; // 三级发展渠道ID

    @JsonProperty("DEV_CHANNEL_FOUR")
    private String devChannelFour; // 四级发展渠道ID

    @JsonProperty("QUICK_REGIST_FLAG")
    private String quickRegistFlag; // 快速创建会员标识
    //CRM_MEMBER_CHANNEL     VARCHAR(64)  REPLACE_IF_NOT_NULL COMMENT '会员天地注册渠道',

    @JsonProperty("CRM_MEMBER_CHANNEL")
    private String crmMemberChannel; // 会员天地注册渠道

    @JsonProperty("FROM_INTEGRATION")
    private Boolean fromIntegration; // 是否来源于23年整合表

    @JsonProperty("FROM_CRM_MEMBER")
    private Boolean fromCrmMember; // 是否来源于会员天地表

    @JsonProperty("FROM_MILE_REGISTER")
    private Boolean fromMileRegister; // 是否来源于常客注册表

    @JsonProperty("REGIST_COUNT")
    private Integer registCount = 1; // 注册计数

    //@JsonProperty("SOURCE_LAST_UPDATETIME")
    //private String sourceLastUpdatetime; // 源系统最后更新时间（时间戳）

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

    public String getFkTid() {
        return fkTid;
    }

    public void setFkTid(String fkTid) {
        this.fkTid = fkTid;
    }

    public String getFkRegisterTime() {
        return fkRegisterTime;
    }

    public void setFkRegisterTime(String fkRegisterTime) {
        this.fkRegisterTime = fkRegisterTime;
    }

    public String getFkRegisterDate() {
        return fkRegisterDate;
    }

    public void setFkRegisterDate(String fkRegisterDate) {
        this.fkRegisterDate = fkRegisterDate;
    }

    public String getFrequentCard() {
        return frequentCard;
    }

    public void setFrequentCard(String frequentCard) {
        this.frequentCard = frequentCard;
    }

    public Boolean getTestMember() {
        return testMember;
    }

    public void setTestMember(Boolean testMember) {
        this.testMember = testMember;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getFirstExchQualifi() {
        return firstExchQualifi;
    }

    public void setFirstExchQualifi(String firstExchQualifi) {
        this.firstExchQualifi = firstExchQualifi;
    }

    public String getCertNumber() {
        return certNumber;
    }

    public void setCertNumber(String certNumber) {
        this.certNumber = certNumber;
    }

    public String getCertType() {
        return certType;
    }

    public void setCertType(String certType) {
        this.certType = certType;
    }

    public String getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(String expireTime) {
        this.expireTime = expireTime;
    }

    public String getIssueTime() {
        return issueTime;
    }

    public void setIssueTime(String issueTime) {
        this.issueTime = issueTime;
    }

    public String getIssueCountry() {
        return issueCountry;
    }

    public void setIssueCountry(String issueCountry) {
        this.issueCountry = issueCountry;
    }

    public String getEnFirstName() {
        return enFirstName;
    }

    public void setEnFirstName(String enFirstName) {
        this.enFirstName = enFirstName;
    }

    public String getEnLastName() {
        return enLastName;
    }

    public void setEnLastName(String enLastName) {
        this.enLastName = enLastName;
    }

    public String getCnName() {
        return cnName;
    }

    public void setCnName(String cnName) {
        this.cnName = cnName;
    }

    public String getChLastName() {
        return chLastName;
    }

    public void setChLastName(String chLastName) {
        this.chLastName = chLastName;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public String getNationality() {
        return nationality;
    }

    public void setNationality(String nationality) {
        this.nationality = nationality;
    }

    public String getContactLanguage() {
        return contactLanguage;
    }

    public void setContactLanguage(String contactLanguage) {
        this.contactLanguage = contactLanguage;
    }

    public String getParentCardNumber() {
        return parentCardNumber;
    }

    public void setParentCardNumber(String parentCardNumber) {
        this.parentCardNumber = parentCardNumber;
    }

    public String getDevStaffId() {
        return devStaffId;
    }

    public void setDevStaffId(String devStaffId) {
        this.devStaffId = devStaffId;
    }

    public String getDevName() {
        return devName;
    }

    public void setDevName(String devName) {
        this.devName = devName;
    }

    public String getDevDepartment() {
        return devDepartment;
    }

    public void setDevDepartment(String devDepartment) {
        this.devDepartment = devDepartment;
    }

    public String getDevChannelOne() {
        return devChannelOne;
    }

    public void setDevChannelOne(String devChannelOne) {
        this.devChannelOne = devChannelOne;
    }

    public String getDevChannelTow() {
        return devChannelTow;
    }

    public void setDevChannelTow(String devChannelTow) {
        this.devChannelTow = devChannelTow;
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

    public String getQuickRegistFlag() {
        return quickRegistFlag;
    }

    public void setQuickRegistFlag(String quickRegistFlag) {
        this.quickRegistFlag = quickRegistFlag;
    }

    public String getCrmMemberChannel() {
        return crmMemberChannel;
    }

    public void setCrmMemberChannel(String crmMemberChannel) {
        this.crmMemberChannel = crmMemberChannel;
    }

    public Boolean getFromIntegration() {
        return fromIntegration;
    }

    public void setFromIntegration(Boolean fromIntegration) {
        this.fromIntegration = fromIntegration;
    }

    public Boolean getFromCrmMember() {
        return fromCrmMember;
    }

    public void setFromCrmMember(Boolean fromCrmMember) {
        this.fromCrmMember = fromCrmMember;
    }

    public Boolean getFromMileRegister() {
        return fromMileRegister;
    }

    public void setFromMileRegister(Boolean fromMileRegister) {
        this.fromMileRegister = fromMileRegister;
    }

    public Integer getRegistCount() {
        return registCount;
    }

    public void setRegistCount(Integer registCount) {
        this.registCount = registCount;
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
        return "MemberRegisterFactModel{" +
                "pkId='" + pkId + '\'' +
                ", fkTid='" + fkTid + '\'' +
                ", fkRegisterTime='" + fkRegisterTime + '\'' +
                ", fkRegisterDate='" + fkRegisterDate + '\'' +
                ", frequentCard='" + frequentCard + '\'' +
                ", testMember=" + testMember +
                ", gender='" + gender + '\'' +
                ", firstExchQualifi='" + firstExchQualifi + '\'' +
                ", certNumber='" + certNumber + '\'' +
                ", certType='" + certType + '\'' +
                ", expireTime='" + expireTime + '\'' +
                ", issueTime='" + issueTime + '\'' +
                ", issueCountry='" + issueCountry + '\'' +
                ", enFirstName='" + enFirstName + '\'' +
                ", enLastName='" + enLastName + '\'' +
                ", cnName='" + cnName + '\'' +
                ", chLastName='" + chLastName + '\'' +
                ", birthday='" + birthday + '\'' +
                ", nationality='" + nationality + '\'' +
                ", contactLanguage='" + contactLanguage + '\'' +
                ", parentCardNumber='" + parentCardNumber + '\'' +
                ", devStaffId='" + devStaffId + '\'' +
                ", devName='" + devName + '\'' +
                ", devDepartment='" + devDepartment + '\'' +
                ", devChannelOne='" + devChannelOne + '\'' +
                ", devChannelTow='" + devChannelTow + '\'' +
                ", devChannelThree='" + devChannelThree + '\'' +
                ", devChannelFour='" + devChannelFour + '\'' +
                ", quickRegistFlag='" + quickRegistFlag + '\'' +
                ", crmMemberChannel='" + crmMemberChannel + '\'' +
                ", fromIntegration=" + fromIntegration +
                ", fromCrmMember=" + fromCrmMember +
                ", fromMileRegister=" + fromMileRegister +
                ", registCount=" + registCount +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
