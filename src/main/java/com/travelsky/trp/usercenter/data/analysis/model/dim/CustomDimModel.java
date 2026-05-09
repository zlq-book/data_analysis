package com.travelsky.trp.usercenter.data.analysis.model.dim;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 直销用户维表
 */
public class CustomDimModel {

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
     * CUSTOMER_ID
     */
    @JsonProperty("CUSTOMER_ID")
    private String customerId;

    /**
     * 直销用户注册渠道
     */
    @JsonProperty("DIRECT_REGISTER_CHANNEL")
    private String directRegisterChannel;

    /**
     * 直销用户状态
     */
    @JsonProperty("DIRECT_USER_STATUS")
    private String directUserStatus;

    /**
     * 直销用户最新登录时间
     */
    @JsonProperty("LAST_LOGIN_TIME")
    private LocalDate lastLoginTime;

    /**
     * 直销用户注册日期
     */
    @JsonProperty("DIRECT_REGISTER_DATE")
    private String directRegisterDate;

    /**
     * 直销实名认证标识
     */
    @JsonProperty("DIRECT_VERIFIED_FLAG")
    private Boolean directVerifiedFlag;

    /**
     * 直销实名认证方式
     */
    @JsonProperty("DIRECT_VERIFIED_METHOD")
    private String directVerifiedMethod;

    /**
     * 直销实名认证日期
     */
    @JsonProperty("DIRECT_VERIFY_DATE")
    private String directVerifyDate;

    /**
     * 是否直销黑名单用户
     */
    @JsonProperty("IS_BLACKLIST_USER")
    private Boolean isBlacklistUser;

    /**
     * 学生标识
     */
    @JsonProperty("STUDENT_FLAG")
    private Boolean studentFlag;

    /**
     * 学生身份有效期
     */
    @JsonProperty("STUDENT_EXPIRY")
    private String studentExpiry;

    /**
     * 教师标识
     */
    @JsonProperty("TEACHER_FLAG")
    private Boolean teacherFlag;

    /**
     * 直销用户收件地址
     */
    @JsonProperty("SHIP_ADDRESS")
    private String shipAddress;

    /**
     * 直销发票抬头
     */
    @JsonProperty("INVOICE_TITLE")
    private String invoiceTitle;

    /**
     * 官网虚耗状态
     */
    @JsonProperty("WEB_VIRTUAL_STATUS")
    private String webVirtualStatus;

    /**
     * 官网账号状态
     */
    @JsonProperty("WEB_ACCOUNT_STATUS")
    private String webAccountStatus;

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

    public String getDirectRegisterChannel() {
        return directRegisterChannel;
    }

    public void setDirectRegisterChannel(String directRegisterChannel) {
        this.directRegisterChannel = directRegisterChannel;
    }

    public String getDirectUserStatus() {
        return directUserStatus;
    }

    public void setDirectUserStatus(String directUserStatus) {
        this.directUserStatus = directUserStatus;
    }

    public String getDirectRegisterDate() {
        return directRegisterDate;
    }

    public void setDirectRegisterDate(String directRegisterDate) {
        this.directRegisterDate = directRegisterDate;
    }

    public String getDirectVerifyDate() {
        return directVerifyDate;
    }

    public void setDirectVerifyDate(String directVerifyDate) {
        this.directVerifyDate = directVerifyDate;
    }

    public Boolean getDirectVerifiedFlag() {
        return directVerifiedFlag;
    }

    public void setDirectVerifiedFlag(Boolean directVerifiedFlag) {
        this.directVerifiedFlag = directVerifiedFlag;
    }

    public String getDirectVerifiedMethod() {
        return directVerifiedMethod;
    }

    public void setDirectVerifiedMethod(String directVerifiedMethod) {
        this.directVerifiedMethod = directVerifiedMethod;
    }


    public Boolean getBlacklistUser() {
        return isBlacklistUser;
    }

    public void setBlacklistUser(Boolean blacklistUser) {
        isBlacklistUser = blacklistUser;
    }

    public Boolean getStudentFlag() {
        return studentFlag;
    }

    public void setStudentFlag(Boolean studentFlag) {
        this.studentFlag = studentFlag;
    }

    public String getStudentExpiry() {
        return studentExpiry;
    }

    public void setStudentExpiry(String studentExpiry) {
        this.studentExpiry = studentExpiry;
    }

    public Boolean getTeacherFlag() {
        return teacherFlag;
    }

    public void setTeacherFlag(Boolean teacherFlag) {
        this.teacherFlag = teacherFlag;
    }

    public String getShipAddress() {
        return shipAddress;
    }

    public void setShipAddress(String shipAddress) {
        this.shipAddress = shipAddress;
    }

    public String getInvoiceTitle() {
        return invoiceTitle;
    }

    public void setInvoiceTitle(String invoiceTitle) {
        this.invoiceTitle = invoiceTitle;
    }

    public String getWebVirtualStatus() {
        return webVirtualStatus;
    }

    public void setWebVirtualStatus(String webVirtualStatus) {
        this.webVirtualStatus = webVirtualStatus;
    }

    public String getWebAccountStatus() {
        return webAccountStatus;
    }

    public void setWebAccountStatus(String webAccountStatus) {
        this.webAccountStatus = webAccountStatus;
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

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public LocalDate getLastLoginTime() {
        return lastLoginTime;
    }

    public void setLastLoginTime(LocalDate lastLoginTime) {
        this.lastLoginTime = lastLoginTime;
    }

    @Override
    public String toString() {
        return "CustomDimModel{" +
                "systemKey='" + systemKey + '\'' +
                ", tId='" + tId + '\'' +
                ", directRegisterChannel='" + directRegisterChannel + '\'' +
                ", directUserStatus='" + directUserStatus + '\'' +
                ", directRegisterDate=" + directRegisterDate +
                ", directVerifiedFlag=" + directVerifiedFlag +
                ", directVerifiedMethod='" + directVerifiedMethod + '\'' +
                ", directVerifyDate=" + directVerifyDate +
                ", isBlacklistUser=" + isBlacklistUser +
                ", studentFlag=" + studentFlag +
                ", studentExpiry=" + studentExpiry +
                ", teacherFlag=" + teacherFlag +
                ", shipAddress='" + shipAddress + '\'' +
                ", invoiceTitle='" + invoiceTitle + '\'' +
                ", webVirtualStatus='" + webVirtualStatus + '\'' +
                ", webAccountStatus='" + webAccountStatus + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}
