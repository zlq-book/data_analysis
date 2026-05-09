package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TOdsLyxOrderTicketInfo {
    @JsonProperty("ID")
    private Long ID;
    @JsonProperty("ORDER_NO")
    private String ORDER_NO;
    @JsonProperty("ORDER_SOURCE")
    private String ORDER_SOURCE;
    @JsonProperty("ORDER_STATUS")
    private String ORDER_STATUS;
    @JsonProperty("LY_NAME")
    private String LY_NAME;
    @JsonProperty("LY_CARD")
    private String LY_CARD;
    @JsonProperty("LY_LEVEL")
    private String LY_LEVEL;
    @JsonProperty("LY_MOBILE")
    private String LY_MOBILE;
    @JsonProperty("ORDER_PERSON")
    private String ORDER_PERSON;
    @JsonProperty("CREATE_TIME")
    private Long CREATE_TIME;
    @JsonProperty("UPDATOR")
    private String UPDATOR;
    @JsonProperty("UPDATE_TIME")
    private Long UPDATE_TIME;
    @JsonProperty("TOTAL_PRICE")
    private String TOTAL_PRICE;
    @JsonProperty("TOTAL_TAX")
    private String TOTAL_TAX;
    @JsonProperty("TOTAL_EXTRA")
    private String TOTAL_EXTRA;
    @JsonProperty("PAY_ID")
    private Long PAY_ID;
    @JsonProperty("USED_LEFT_FLAG")
    private String USED_LEFT_FLAG;
    @JsonProperty("ERROR_STATUS")
    private String ERROR_STATUS;
    @JsonProperty("ERROR_SOLUTION")
    private String ERROR_SOLUTION;

    @Override
    public String toString() {
        return "TOdsLyxOrderTicketInfo{" +
                "ID=" + ID +
                ", ORDER_NO='" + ORDER_NO + '\'' +
                ", ORDER_SOURCE='" + ORDER_SOURCE + '\'' +
                ", ORDER_STATUS='" + ORDER_STATUS + '\'' +
                ", LY_NAME='" + LY_NAME + '\'' +
                ", LY_CARD='" + LY_CARD + '\'' +
                ", LY_LEVEL='" + LY_LEVEL + '\'' +
                ", LY_MOBILE='" + LY_MOBILE + '\'' +
                ", ORDER_PERSON='" + ORDER_PERSON + '\'' +
                ", CREATE_TIME=" + CREATE_TIME +
                ", UPDATOR='" + UPDATOR + '\'' +
                ", UPDATE_TIME=" + UPDATE_TIME +
                ", TOTAL_PRICE='" + TOTAL_PRICE + '\'' +
                ", TOTAL_TAX='" + TOTAL_TAX + '\'' +
                ", TOTAL_EXTRA='" + TOTAL_EXTRA + '\'' +
                ", PAY_ID=" + PAY_ID +
                ", USED_LEFT_FLAG='" + USED_LEFT_FLAG + '\'' +
                ", ERROR_STATUS='" + ERROR_STATUS + '\'' +
                ", ERROR_SOLUTION='" + ERROR_SOLUTION + '\'' +
                '}';
    }

    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public String getORDER_NO() {
        return ORDER_NO;
    }

    public void setORDER_NO(String ORDER_NO) {
        this.ORDER_NO = ORDER_NO;
    }

    public String getORDER_SOURCE() {
        return ORDER_SOURCE;
    }

    public void setORDER_SOURCE(String ORDER_SOURCE) {
        this.ORDER_SOURCE = ORDER_SOURCE;
    }

    public String getORDER_STATUS() {
        return ORDER_STATUS;
    }

    public void setORDER_STATUS(String ORDER_STATUS) {
        this.ORDER_STATUS = ORDER_STATUS;
    }

    public String getLY_NAME() {
        return LY_NAME;
    }

    public void setLY_NAME(String LY_NAME) {
        this.LY_NAME = LY_NAME;
    }

    public String getLY_CARD() {
        return LY_CARD;
    }

    public void setLY_CARD(String LY_CARD) {
        this.LY_CARD = LY_CARD;
    }

    public String getLY_LEVEL() {
        return LY_LEVEL;
    }

    public void setLY_LEVEL(String LY_LEVEL) {
        this.LY_LEVEL = LY_LEVEL;
    }

    public String getLY_MOBILE() {
        return LY_MOBILE;
    }

    public void setLY_MOBILE(String LY_MOBILE) {
        this.LY_MOBILE = LY_MOBILE;
    }

    public String getORDER_PERSON() {
        return ORDER_PERSON;
    }

    public void setORDER_PERSON(String ORDER_PERSON) {
        this.ORDER_PERSON = ORDER_PERSON;
    }

    public Long getCREATE_TIME() {
        return CREATE_TIME;
    }

    public void setCREATE_TIME(Long CREATE_TIME) {
        this.CREATE_TIME = CREATE_TIME;
    }

    public String getUPDATOR() {
        return UPDATOR;
    }

    public void setUPDATOR(String UPDATOR) {
        this.UPDATOR = UPDATOR;
    }

    public Long getUPDATE_TIME() {
        return UPDATE_TIME;
    }

    public void setUPDATE_TIME(Long UPDATE_TIME) {
        this.UPDATE_TIME = UPDATE_TIME;
    }

    public String getTOTAL_PRICE() {
        return TOTAL_PRICE;
    }

    public void setTOTAL_PRICE(String TOTAL_PRICE) {
        this.TOTAL_PRICE = TOTAL_PRICE;
    }

    public String getTOTAL_TAX() {
        return TOTAL_TAX;
    }

    public void setTOTAL_TAX(String TOTAL_TAX) {
        this.TOTAL_TAX = TOTAL_TAX;
    }

    public String getTOTAL_EXTRA() {
        return TOTAL_EXTRA;
    }

    public void setTOTAL_EXTRA(String TOTAL_EXTRA) {
        this.TOTAL_EXTRA = TOTAL_EXTRA;
    }

    public Long getPAY_ID() {
        return PAY_ID;
    }

    public void setPAY_ID(Long PAY_ID) {
        this.PAY_ID = PAY_ID;
    }

    public String getUSED_LEFT_FLAG() {
        return USED_LEFT_FLAG;
    }

    public void setUSED_LEFT_FLAG(String USED_LEFT_FLAG) {
        this.USED_LEFT_FLAG = USED_LEFT_FLAG;
    }

    public String getERROR_STATUS() {
        return ERROR_STATUS;
    }

    public void setERROR_STATUS(String ERROR_STATUS) {
        this.ERROR_STATUS = ERROR_STATUS;
    }

    public String getERROR_SOLUTION() {
        return ERROR_SOLUTION;
    }

    public void setERROR_SOLUTION(String ERROR_SOLUTION) {
        this.ERROR_SOLUTION = ERROR_SOLUTION;
    }
}
