package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TOdsLyxOrderPayInfo {
    @JsonProperty("ID")
    private Long ID;
    @JsonProperty("LY_VALUE_STATUS")
    private String LY_VALUE_STATUS;
    @JsonProperty("LY_SUM")
    private String LY_SUM;
    @JsonProperty("LY_PAY_TIME")
    private Long LY_PAY_TIME;
    @JsonProperty("CASH_PAY_STATUS")
    private String CASH_PAY_STATUS;
    @JsonProperty("PAY_WAY")
    private String PAY_WAY;
    @JsonProperty("CASH_SUM")
    private String CASH_SUM;
    @JsonProperty("CASH_PAY_NO")
    private String CASH_PAY_NO;
    @JsonProperty("CASH_PAY_MOBILE")
    private String CASH_PAY_MOBILE;
    @JsonProperty("CASH_PAY_TIME")
    private Long CASH_PAY_TIME;
    @JsonProperty("CREATE_TIME")
    private Long CREATE_TIME;
    @JsonProperty("UPDATE_TIME")
    private Long UPDATE_TIME;
    @JsonProperty("PAY_SOURCE")
    private String PAY_SOURCE;
    @JsonProperty("CASH_FAIL_REASON")
    private String CASH_FAIL_REASON;
    @JsonProperty("LY_FAIL_REASON")
    private String LY_FAIL_REASON;
    @JsonProperty("ORDER_TYPE")
    private String ORDER_TYPE;
    @JsonProperty("CASHPAY_STARTTIME")
    private Long CASHPAY_STARTTIME;
    @JsonProperty("ORIGIN_TYPE")
    private String ORIGIN_TYPE;

    @Override
    public String toString() {
        return "TOdsLyxOrderPayInfo{" +
                "ID=" + ID +
                ", LY_VALUE_STATUS='" + LY_VALUE_STATUS + '\'' +
                ", LY_SUM='" + LY_SUM + '\'' +
                ", LY_PAY_TIME=" + LY_PAY_TIME +
                ", CASH_PAY_STATUS='" + CASH_PAY_STATUS + '\'' +
                ", PAY_WAY='" + PAY_WAY + '\'' +
                ", CASH_SUM='" + CASH_SUM + '\'' +
                ", CASH_PAY_NO='" + CASH_PAY_NO + '\'' +
                ", CASH_PAY_MOBILE='" + CASH_PAY_MOBILE + '\'' +
                ", CASH_PAY_TIME=" + CASH_PAY_TIME +
                ", CREATE_TIME=" + CREATE_TIME +
                ", UPDATE_TIME=" + UPDATE_TIME +
                ", PAY_SOURCE='" + PAY_SOURCE + '\'' +
                ", CASH_FAIL_REASON='" + CASH_FAIL_REASON + '\'' +
                ", LY_FAIL_REASON='" + LY_FAIL_REASON + '\'' +
                ", ORDER_TYPE='" + ORDER_TYPE + '\'' +
                ", CASHPAY_STARTTIME=" + CASHPAY_STARTTIME +
                ", ORIGIN_TYPE='" + ORIGIN_TYPE + '\'' +
                '}';
    }

    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public String getLY_VALUE_STATUS() {
        return LY_VALUE_STATUS;
    }

    public void setLY_VALUE_STATUS(String LY_VALUE_STATUS) {
        this.LY_VALUE_STATUS = LY_VALUE_STATUS;
    }

    public String getLY_SUM() {
        return LY_SUM;
    }

    public void setLY_SUM(String LY_SUM) {
        this.LY_SUM = LY_SUM;
    }

    public Long getLY_PAY_TIME() {
        return LY_PAY_TIME;
    }

    public void setLY_PAY_TIME(Long LY_PAY_TIME) {
        this.LY_PAY_TIME = LY_PAY_TIME;
    }

    public String getCASH_PAY_STATUS() {
        return CASH_PAY_STATUS;
    }

    public void setCASH_PAY_STATUS(String CASH_PAY_STATUS) {
        this.CASH_PAY_STATUS = CASH_PAY_STATUS;
    }

    public String getPAY_WAY() {
        return PAY_WAY;
    }

    public void setPAY_WAY(String PAY_WAY) {
        this.PAY_WAY = PAY_WAY;
    }

    public String getCASH_SUM() {
        return CASH_SUM;
    }

    public void setCASH_SUM(String CASH_SUM) {
        this.CASH_SUM = CASH_SUM;
    }

    public String getCASH_PAY_NO() {
        return CASH_PAY_NO;
    }

    public void setCASH_PAY_NO(String CASH_PAY_NO) {
        this.CASH_PAY_NO = CASH_PAY_NO;
    }

    public String getCASH_PAY_MOBILE() {
        return CASH_PAY_MOBILE;
    }

    public void setCASH_PAY_MOBILE(String CASH_PAY_MOBILE) {
        this.CASH_PAY_MOBILE = CASH_PAY_MOBILE;
    }

    public Long getCASH_PAY_TIME() {
        return CASH_PAY_TIME;
    }

    public void setCASH_PAY_TIME(Long CASH_PAY_TIME) {
        this.CASH_PAY_TIME = CASH_PAY_TIME;
    }

    public Long getCREATE_TIME() {
        return CREATE_TIME;
    }

    public void setCREATE_TIME(Long CREATE_TIME) {
        this.CREATE_TIME = CREATE_TIME;
    }

    public Long getUPDATE_TIME() {
        return UPDATE_TIME;
    }

    public void setUPDATE_TIME(Long UPDATE_TIME) {
        this.UPDATE_TIME = UPDATE_TIME;
    }

    public String getPAY_SOURCE() {
        return PAY_SOURCE;
    }

    public void setPAY_SOURCE(String PAY_SOURCE) {
        this.PAY_SOURCE = PAY_SOURCE;
    }

    public String getCASH_FAIL_REASON() {
        return CASH_FAIL_REASON;
    }

    public void setCASH_FAIL_REASON(String CASH_FAIL_REASON) {
        this.CASH_FAIL_REASON = CASH_FAIL_REASON;
    }

    public String getLY_FAIL_REASON() {
        return LY_FAIL_REASON;
    }

    public void setLY_FAIL_REASON(String LY_FAIL_REASON) {
        this.LY_FAIL_REASON = LY_FAIL_REASON;
    }

    public String getORDER_TYPE() {
        return ORDER_TYPE;
    }

    public void setORDER_TYPE(String ORDER_TYPE) {
        this.ORDER_TYPE = ORDER_TYPE;
    }

    public Long getCASHPAY_STARTTIME() {
        return CASHPAY_STARTTIME;
    }

    public void setCASHPAY_STARTTIME(Long CASHPAY_STARTTIME) {
        this.CASHPAY_STARTTIME = CASHPAY_STARTTIME;
    }

    public String getORIGIN_TYPE() {
        return ORIGIN_TYPE;
    }

    public void setORIGIN_TYPE(String ORIGIN_TYPE) {
        this.ORIGIN_TYPE = ORIGIN_TYPE;
    }
}
