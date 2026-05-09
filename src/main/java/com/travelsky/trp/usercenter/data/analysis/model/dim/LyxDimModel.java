package com.travelsky.trp.usercenter.data.analysis.model.dim;

import java.time.LocalDate;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 鲁雁行用户维表
 */
public class LyxDimModel {

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
     * 鲁雁行用户ID
     */
    @JsonProperty("LY_USER_ID")
    private String lyUserId;

    /**
     * 鲁雁行注册时间
     */
    @JsonProperty("LY_REGISTER_TIME")
    private String lyRegisterTime;

    /**
     * 鲁雁行卡号
     */
    @JsonProperty("LY_CARD_NUMBER")
    private String lyCardNumber;

    /**
     * 鲁雁行用户等级
     */
    @JsonProperty("LY_USER_LEVEL")
    private String lyUserLevel;

    /**
     * 鲁雁行用户状态
     */
    @JsonProperty("LY_USER_STATUS")
    private String lyUserStatus;

    /**
     * 鲁雁行注册状态
     */
    @JsonProperty("LY_REGISTER_STATUS")
    private String lyRegisterStatus;

    /**
     * 鲁雁行实名认证状态
     */
    @JsonProperty("LY_VERIFY_STATUS")
    private String lyVerifyStatus;

    /**
     * 鲁雁行实名认证方式
     */
    @JsonProperty("LY_VERIFY_METHOD")
    private String lyVerifyMethod;

    /**
     * 鲁雁行实名认证时间
     */
    @JsonProperty("LY_VERIFY_TIME")
    private String lyVerifyTime;

    /**
     * 鲁雁行是否需要LIP三要素认证
     */
    @JsonProperty("LY_NEED_LIP_VERIFY")
    private Boolean lyNeedLipVerify;

    /**
     * 鲁雁行LIP三要素认证结果
     */
    @JsonProperty("LY_LIP_VERIFY_RESULT")
    private Boolean lyLipVerifyResult;

    /**
     * 鲁雁行等级变更日期
     */
    @JsonProperty("LY_LEVEL_CHANGE_DATE")
    private LocalDate lyLevelChangeDate;

    /**
     * 变更前鲁雁行等级
     */
    @JsonProperty("LY_PREV_LEVEL")
    private String lyPrevLevel;

    /**
     * 生命周期鲁雁值
     */
    @JsonProperty("LY_LIFETIME_POINTS")
    private Integer lyLifetimePoints;

    /**
     * 可用鲁雁值
     */
    @JsonProperty("LY_AVAILABLE_POINTS")
    private Integer lyAvailablePoints;

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

    public String getLyUserId() {
        return lyUserId;
    }

    public void setLyUserId(String lyUserId) {
        this.lyUserId = lyUserId;
    }

    public String getLyRegisterTime() {
        return lyRegisterTime;
    }

    public void setLyRegisterTime(String lyRegisterTime) {
        this.lyRegisterTime = lyRegisterTime;
    }

    public String getLyCardNumber() {
        return lyCardNumber;
    }

    public void setLyCardNumber(String lyCardNumber) {
        this.lyCardNumber = lyCardNumber;
    }

    public String getLyUserLevel() {
        return lyUserLevel;
    }

    public void setLyUserLevel(String lyUserLevel) {
        this.lyUserLevel = lyUserLevel;
    }

    public String getLyUserStatus() {
        return lyUserStatus;
    }

    public void setLyUserStatus(String lyUserStatus) {
        this.lyUserStatus = lyUserStatus;
    }

    public String getLyRegisterStatus() {
        return lyRegisterStatus;
    }

    public void setLyRegisterStatus(String lyRegisterStatus) {
        this.lyRegisterStatus = lyRegisterStatus;
    }

    public String getLyVerifyStatus() {
        return lyVerifyStatus;
    }

    public void setLyVerifyStatus(String lyVerifyStatus) {
        this.lyVerifyStatus = lyVerifyStatus;
    }

    public String getLyVerifyMethod() {
        return lyVerifyMethod;
    }

    public void setLyVerifyMethod(String lyVerifyMethod) {
        this.lyVerifyMethod = lyVerifyMethod;
    }

    public String getLyVerifyTime() {
        return lyVerifyTime;
    }

    public void setLyVerifyTime(String lyVerifyTime) {
        this.lyVerifyTime = lyVerifyTime;
    }

    public Boolean getLyNeedLipVerify() {
        return lyNeedLipVerify;
    }

    public void setLyNeedLipVerify(Boolean lyNeedLipVerify) {
        this.lyNeedLipVerify = lyNeedLipVerify;
    }

    public Boolean getLyLipVerifyResult() {
        return lyLipVerifyResult;
    }

    public void setLyLipVerifyResult(Boolean lyLipVerifyResult) {
        this.lyLipVerifyResult = lyLipVerifyResult;
    }

    public LocalDate getLyLevelChangeDate() {
        return lyLevelChangeDate;
    }

    public void setLyLevelChangeDate(LocalDate lyLevelChangeDate) {
        this.lyLevelChangeDate = lyLevelChangeDate;
    }

    public String getLyPrevLevel() {
        return lyPrevLevel;
    }

    public void setLyPrevLevel(String lyPrevLevel) {
        this.lyPrevLevel = lyPrevLevel;
    }

    public Integer getLyLifetimePoints() {
        return lyLifetimePoints;
    }

    public void setLyLifetimePoints(Integer lyLifetimePoints) {
        this.lyLifetimePoints = lyLifetimePoints;
    }

    public Integer getLyAvailablePoints() {
        return lyAvailablePoints;
    }

    public void setLyAvailablePoints(Integer lyAvailablePoints) {
        this.lyAvailablePoints = lyAvailablePoints;
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
        return "LyxDimModel{" +
                "systemKey='" + systemKey + '\'' +
                ", tId='" + tId + '\'' +
                ", lyUserId='" + lyUserId + '\'' +
                ", lyRegisterTime='" + lyRegisterTime + '\'' +
                ", lyCardNumber='" + lyCardNumber + '\'' +
                ", lyUserLevel='" + lyUserLevel + '\'' +
                ", lyUserStatus='" + lyUserStatus + '\'' +
                ", lyRegisterStatus='" + lyRegisterStatus + '\'' +
                ", lyVerifyStatus='" + lyVerifyStatus + '\'' +
                ", lyVerifyMethod='" + lyVerifyMethod + '\'' +
                ", lyVerifyTime='" + lyVerifyTime + '\'' +
                ", lyNeedLipVerify=" + lyNeedLipVerify +
                ", lyLipVerifyResult=" + lyLipVerifyResult +
                ", lyLevelChangeDate=" + lyLevelChangeDate +
                ", lyPrevLevel='" + lyPrevLevel + '\'' +
                ", lyLifetimePoints=" + lyLifetimePoints +
                ", lyAvailablePoints=" + lyAvailablePoints +
                ", updateTime='" + updateTime + '\'' +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}
