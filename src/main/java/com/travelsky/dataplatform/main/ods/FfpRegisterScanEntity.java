package com.travelsky.dataplatform.main.ods;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;

public class FfpRegisterScanEntity implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String id;
    private String tradeid;
    private Date createTime;
    private String cnname;
    private String fltdate;
    private String fltnum;
    private String reservedSeat;
    private String fphone;
    private String fcardType;
    private String matchFlag;
    private String ffpRegisterInfoId;
    private String remarks;
    private Timestamp etlCreateTime;
    private Timestamp etlUpdateTime;
    private Date etlDate;
    
    public FfpRegisterScanEntity() {
    }
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getTradeid() {
        return tradeid;
    }
    
    public void setTradeid(String tradeid) {
        this.tradeid = tradeid;
    }
    
    public Date getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
    
    public String getCnname() {
        return cnname;
    }
    
    public void setCnname(String cnname) {
        this.cnname = cnname;
    }
    
    public String getFltdate() {
        return fltdate;
    }
    
    public void setFltdate(String fltdate) {
        this.fltdate = fltdate;
    }
    
    public String getFltnum() {
        return fltnum;
    }
    
    public void setFltnum(String fltnum) {
        this.fltnum = fltnum;
    }
    
    public String getReservedSeat() {
        return reservedSeat;
    }
    
    public void setReservedSeat(String reservedSeat) {
        this.reservedSeat = reservedSeat;
    }
    
    public String getFphone() {
        return fphone;
    }
    
    public void setFphone(String fphone) {
        this.fphone = fphone;
    }
    
    public String getFcardType() {
        return fcardType;
    }
    
    public void setFcardType(String fcardType) {
        this.fcardType = fcardType;
    }
    
    public String getMatchFlag() {
        return matchFlag;
    }
    
    public void setMatchFlag(String matchFlag) {
        this.matchFlag = matchFlag;
    }
    
    public String getFfpRegisterInfoId() {
        return ffpRegisterInfoId;
    }
    
    public void setFfpRegisterInfoId(String ffpRegisterInfoId) {
        this.ffpRegisterInfoId = ffpRegisterInfoId;
    }
    
    public String getRemarks() {
        return remarks;
    }
    
    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }
    
    public Timestamp getEtlCreateTime() {
        return etlCreateTime;
    }
    
    public void setEtlCreateTime(Timestamp etlCreateTime) {
        this.etlCreateTime = etlCreateTime;
    }
    
    public Timestamp getEtlUpdateTime() {
        return etlUpdateTime;
    }
    
    public void setEtlUpdateTime(Timestamp etlUpdateTime) {
        this.etlUpdateTime = etlUpdateTime;
    }
    
    public Date getEtlDate() {
        return etlDate;
    }
    
    public void setEtlDate(Date etlDate) {
        this.etlDate = etlDate;
    }
}
