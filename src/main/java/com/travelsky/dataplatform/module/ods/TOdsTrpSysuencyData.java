package com.travelsky.dataplatform.module.ods;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * 对应 Doris 表 T_ODS_TRP_SYSUENCY_DATA 的实体类
 */
public class TOdsTrpSysuencyData {

    /**
     * 主键
     */
    @JsonProperty("TRP_SYS_ID")
    private String trpSysId;

    /**
     * trp系统数据pnr编号
     */
    @JsonProperty("TRP_SYS_PNR")
    private String trpSysPnr;

    /**
     * trp系统数据spnr编号
     */
    @JsonProperty("TRP_SYS_SPNR")
    private String trpSysSpnr;

    /**
     * 数据创建精确时间（毫秒级）
     */
    @JsonProperty("TRP_SYS_CREATE_TIME")
    private String trpSysCreateTime;

    /**
     * 数据创建日期（按天分区）
     */
    @JsonProperty("TRP_SYS_CREATE_DATE")
    private String trpSysCreateDate;

    /**
     * XML原始数据
     */
    @JsonProperty("TRP_SYS_DATA")
    private String trpSysData;

    // Getters and Setters
    public String getTrpSysId() {
        return trpSysId;
    }

    public void setTrpSysId(String trpSysId) {
        this.trpSysId = trpSysId;
    }

    public String getTrpSysPnr() {
        return trpSysPnr;
    }

    public void setTrpSysPnr(String trpSysPnr) {
        this.trpSysPnr = trpSysPnr;
    }

    public String getTrpSysSpnr() {
        return trpSysSpnr;
    }

    public void setTrpSysSpnr(String trpSysSpnr) {
        this.trpSysSpnr = trpSysSpnr;
    }

    public String getTrpSysCreateTime() {
        return trpSysCreateTime;
    }

    public void setTrpSysCreateTime(String trpSysCreateTime) {
        this.trpSysCreateTime = trpSysCreateTime;
    }

    public String getTrpSysCreateDate() {
        return trpSysCreateDate;
    }

    public void setTrpSysCreateDate(String trpSysCreateDate) {
        this.trpSysCreateDate = trpSysCreateDate;
    }

    public String getTrpSysData() {
        return trpSysData;
    }

    public void setTrpSysData(String trpSysData) {
        this.trpSysData = trpSysData;
    }

    @Override
    public String toString() {
        return "{"
                + "TOdsTrpSysuencyData: {"
                + "trpSysId: " + trpSysId
                + ", " + "trpSysPnr: " + trpSysPnr
                + ", " + "trpSysSpnr: " + trpSysSpnr
                + ", " + "trpSysCreateTime: " + trpSysCreateTime
                + ", " + "trpSysCreateDate: " + trpSysCreateDate
                + ", " + "trpSysData: " + trpSysData
                + "}"
                + "}";
    }
}
