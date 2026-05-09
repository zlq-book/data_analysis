package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * 直销注册事实表实体类-T_DWD_REGISTER_UDO_FACT
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/20  14:04
 */
public class RegisterUdoFactModel {

    /**
     * 用户customerID
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 注册用户
     */
    @JsonProperty("FK_REGISTER_USER")
    private String fkRegisterUser;

    /**
     * 注册日期
     */
    @JsonProperty("FK_REGISTER_DATE")
    private String fkRegisterDate;

    /**
     * 注册时间
     */
    @JsonProperty("REGISTER_TIME")
    private String registerTime;

    /**
     * 直销用户注册渠道
     */
    @JsonProperty("AK_REGISTER_CHANNEL")
    private String akRegisterChannel;

    /**
     * 注册计数
     */
    @JsonProperty("REGISTER_COUNT")
    private Integer registerCount = 1;

    /**
     * 数据是否启用
     */
    @JsonProperty("DATA_ACTIVE")
    private Boolean dataActive;

    /**
     * 数据启用时间
     */
    @JsonProperty("DATA_ACTIVE_TIME")
    private String dataActiveTime;

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

    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getFkRegisterUser() {
        return fkRegisterUser;
    }

    public void setFkRegisterUser(String fkRegisterUser) {
        this.fkRegisterUser = fkRegisterUser;
    }

    public String getFkRegisterDate() {
        return fkRegisterDate;
    }

    public void setFkRegisterDate(String fkRegisterDate) {
        this.fkRegisterDate = fkRegisterDate;
    }

    public String getRegisterTime() {
        return registerTime;
    }

    public void setRegisterTime(String registerTime) {
        this.registerTime = registerTime;
    }

    public String getAkRegisterChannel() {
        return akRegisterChannel;
    }

    public void setAkRegisterChannel(String akRegisterChannel) {
        this.akRegisterChannel = akRegisterChannel;
    }

    public Integer getRegisterCount() {
        return registerCount;
    }

    public void setRegisterCount(Integer registerCount) {
        this.registerCount = registerCount;
    }

    public Boolean getDataActive() {
        return dataActive;
    }

    public void setDataActive(Boolean dataActive) {
        this.dataActive = dataActive;
    }

    public String getDataActiveTime() {
        return dataActiveTime;
    }

    public void setDataActiveTime(String dataActiveTime) {
        this.dataActiveTime = dataActiveTime;
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

    @Override
    public String toString() {
        return "{"
                + "RegisterUdoFactModel: {"
                + "pkId: " + pkId
                + ", " + "fkRegisterUser: " + fkRegisterUser
                + ", " + "fkRegisterDate: " + fkRegisterDate
                + ", " + "registerTime: " + registerTime
                + ", " + "akRegisterChannel: " + akRegisterChannel
                + ", " + "registerCount: " + registerCount
                + ", " + "dataActive: " + dataActive
                + ", " + "dataActiveTime: " + dataActiveTime
                + ", " + "sourceLastUpdatetime: " + sourceLastUpdatetime
                + ", " + "systemCreatetime: " + systemCreatetime
                + ", " + "systemLastUpdatetime: " + systemLastUpdatetime
                + "}"
                + "}";
    }
}