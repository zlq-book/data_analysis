package com.travelsky.trp.usercenter.data.analysis.model.dim;


import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 高端旅客类型维表
 */
public class GdlkTypeDimModel {

    /**
     * 主键
     */
    @JsonProperty("SYSTEM_KEY")
    private String systemKey;

    /**
     * 高端旅客ID
     */
    @JsonProperty("FK_LY_VIP_ID")
    private String fkLyVipId;

    /**
     * 用户ID
     */
    @JsonProperty("T_ID")
    private String tId;

    /**
     * 高端旅客类型
     */
    @JsonProperty("HIGH_TRAVELER_TYPE")
    private String highTravelerType;

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

    /**
     * 高端旅客code
     */
    @JsonProperty("HIGH_TRAVELER_CODE")
    private String highTravelerCode;

    /**
     * 高端旅客类型id
     */
    @JsonProperty("HIGH_TRAVELER_TYPE_ID")
    private String highTravelerTypeId;

    public String getSystemKey() {
        return systemKey;
    }

    public void setSystemKey(String systemKey) {
        this.systemKey = systemKey;
    }

    public String getFkLyVipId() {
        return fkLyVipId;
    }

    public void setFkLyVipId(String fkLyVipId) {
        this.fkLyVipId = fkLyVipId;
    }

    public String gettId() {
        return tId;
    }

    public void settId(String tId) {
        this.tId = tId;
    }

    public String getHighTravelerType() {
        return highTravelerType;
    }

    public void setHighTravelerType(String highTravelerType) {
        this.highTravelerType = highTravelerType;
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

    public String getHighTravelerCode() {
        return highTravelerCode;
    }

    public void setHighTravelerCode(String highTravelerCode) {
        this.highTravelerCode = highTravelerCode;
    }

    public String getHighTravelerTypeId() {
        return highTravelerTypeId;
    }

    public void setHighTravelerTypeId(String highTravelerTypeId) {
        this.highTravelerTypeId = highTravelerTypeId;
    }

    @Override
    public String toString() {
        return "GdlkDimModel{" +
                "systemKey='" + systemKey + '\'' +
                ", fkLyVipId='" + fkLyVipId + '\'' +
                ", tId='" + tId + '\'' +
                ", highTravelerType='" + highTravelerType + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", createTime='" + createTime + '\'' +
                ", highTravelerCode='" + highTravelerCode + '\'' +
                ", highTravelerTypeId='" + highTravelerTypeId + '\'' +
                '}';
    }
}
