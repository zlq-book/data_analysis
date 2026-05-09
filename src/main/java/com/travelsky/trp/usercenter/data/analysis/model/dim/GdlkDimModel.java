package com.travelsky.trp.usercenter.data.analysis.model.dim;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 高端旅客维表实体类 - T_DIM_GDLK_DIM
 * @date 2025/8/20
 */
public class GdlkDimModel {

    /**
     * 主键
     */
    @JsonProperty("SYSTEM_KEY")
    private String systemKey;

    /**
     * 用户T_ID
     */
    @JsonProperty("FK_USER_TID")
    private String fkUserTid;

    /**
     * 高端旅客类型
     */
    @JsonProperty("HIGH_TRAVELER_TYPE")
    private String highTravelerType;

    /**
     * 高端旅客分层类型
     */
    @JsonProperty("HIGH_TRAVELER_TIER")
    private String highTravelerTier;

    /**
     * 至尊身份分层有效期
     */
    @JsonProperty("TIER_PRESTIGE_EXPIREDATE")
    private String tierPrestigeExpiredate;

    /**
     * 荣耀身份分层有效期
     */
    @JsonProperty("TIER_HONOR_EXPIREDATE")
    private String tierHonorExpiredate;

    /**
     * 高端旅客数据来源
     */
    @JsonProperty("HIGH_TRAVELER_DS")
    private String highTravelerDs;

    /**
     * VIP标识
     */
    @JsonProperty("VIP_FLAG")
    private Boolean vipFlag;

    /**
     * CIP标识
     */
    @JsonProperty("CIP_FLAG")
    private Boolean cipFlag;

    /**
     * VVIP标识
     */
    @JsonProperty("VVIP_FLAG")
    private Boolean vvipFlag;

    /**
     * 餐食喜好
     */
    @JsonProperty("FOOD_PREFERENCE")
    private String foodPreference;

    /**
     * 座位喜好
     */
    @JsonProperty("SEAT_PREFERENCE")
    private String seatPreference;

    /**
     * 临时餐食喜好
     */
    @JsonProperty("TEMP_FOOD_PREFERENCE")
    private String tempFoodPreference;

    /**
     * 临时座位喜好
     */
    @JsonProperty("TEMP_SEAT_PREFERENCE")
    private String tempSeatPreference;

    /**
     * 饮品喜好
     */
    @JsonProperty("BEVERAGE_PREFERENCE")
    private String beveragePreference;

    /**
     * 长期座位喜好
     */
    @JsonProperty("LONG_TERM_SEAT_PREFERENCE")
    private String longTermSeatPreference;

    /**
     * 头等舱休息室喜好
     */
    @JsonProperty("FIRST_CLASS_LOUNGE_PREFERENCE")
    private String firstClassLoungePreference;

    /**
     * 更新时间（含毫秒）
     */
    @JsonProperty("UPDATE_TIME")
    private String updateTime;

    /**
     * 创建时间（含毫秒）
     */
    @JsonProperty("CREATE_TIME")
    private String createTime;

    public String getSystemKey() {
        return systemKey;
    }

    public void setSystemKey(String systemKey) {
        this.systemKey = systemKey;
    }

    public String getFkUserTid() {
        return fkUserTid;
    }

    public void setFkUserTid(String fkUserTid) {
        this.fkUserTid = fkUserTid;
    }

    public String getHighTravelerType() {
        return highTravelerType;
    }

    public void setHighTravelerType(String highTravelerType) {
        this.highTravelerType = highTravelerType;
    }

    public String getHighTravelerTier() {
        return highTravelerTier;
    }

    public void setHighTravelerTier(String highTravelerTier) {
        this.highTravelerTier = highTravelerTier;
    }

    public String getTierPrestigeExpiredate() {
        return tierPrestigeExpiredate;
    }

    public void setTierPrestigeExpiredate(String tierPrestigeExpiredate) {
        this.tierPrestigeExpiredate = tierPrestigeExpiredate;
    }

    public String getTierHonorExpiredate() {
        return tierHonorExpiredate;
    }

    public void setTierHonorExpiredate(String tierHonorExpiredate) {
        this.tierHonorExpiredate = tierHonorExpiredate;
    }

    public String getHighTravelerDs() {
        return highTravelerDs;
    }

    public void setHighTravelerDs(String highTravelerDs) {
        this.highTravelerDs = highTravelerDs;
    }

    public Boolean getVipFlag() {
        return vipFlag;
    }

    public void setVipFlag(Boolean vipFlag) {
        this.vipFlag = vipFlag;
    }

    public Boolean getCipFlag() {
        return cipFlag;
    }

    public void setCipFlag(Boolean cipFlag) {
        this.cipFlag = cipFlag;
    }

    public Boolean getVvipFlag() {
        return vvipFlag;
    }

    public void setVvipFlag(Boolean vvipFlag) {
        this.vvipFlag = vvipFlag;
    }

    public String getFoodPreference() {
        return foodPreference;
    }

    public void setFoodPreference(String foodPreference) {
        this.foodPreference = foodPreference;
    }

    public String getSeatPreference() {
        return seatPreference;
    }

    public void setSeatPreference(String seatPreference) {
        this.seatPreference = seatPreference;
    }

    public String getTempFoodPreference() {
        return tempFoodPreference;
    }

    public void setTempFoodPreference(String tempFoodPreference) {
        this.tempFoodPreference = tempFoodPreference;
    }

    public String getTempSeatPreference() {
        return tempSeatPreference;
    }

    public void setTempSeatPreference(String tempSeatPreference) {
        this.tempSeatPreference = tempSeatPreference;
    }

    public String getBeveragePreference() {
        return beveragePreference;
    }

    public void setBeveragePreference(String beveragePreference) {
        this.beveragePreference = beveragePreference;
    }

    public String getLongTermSeatPreference() {
        return longTermSeatPreference;
    }

    public void setLongTermSeatPreference(String longTermSeatPreference) {
        this.longTermSeatPreference = longTermSeatPreference;
    }

    public String getFirstClassLoungePreference() {
        return firstClassLoungePreference;
    }

    public void setFirstClassLoungePreference(String firstClassLoungePreference) {
        this.firstClassLoungePreference = firstClassLoungePreference;
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
        return "GdlkDimModel{" +
                "systemKey='" + systemKey + '\'' +
                ", fkUserTid='" + fkUserTid + '\'' +
                ", highTravelerType='" + highTravelerType + '\'' +
                ", highTravelerTier='" + highTravelerTier + '\'' +
                ", tierPrestigeExpiredate='" + tierPrestigeExpiredate + '\'' +
                ", tierHonorExpiredate='" + tierHonorExpiredate + '\'' +
                ", highTravelerDs='" + highTravelerDs + '\'' +
                ", vipFlag=" + vipFlag +
                ", cipFlag=" + cipFlag +
                ", vvipFlag=" + vvipFlag +
                ", foodPreference='" + foodPreference + '\'' +
                ", seatPreference='" + seatPreference + '\'' +
                ", tempFoodPreference='" + tempFoodPreference + '\'' +
                ", tempSeatPreference='" + tempSeatPreference + '\'' +
                ", beveragePreference='" + beveragePreference + '\'' +
                ", longTermSeatPreference='" + longTermSeatPreference + '\'' +
                ", firstClassLoungePreference='" + firstClassLoungePreference + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}
