package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TOdsLyxOrderPassengerInfo {
    @JsonProperty("ID")
    private Long ID;
    @JsonProperty("PSG_NAME_CN")
    private String PSG_NAME_CN;
    @JsonProperty("PSG_NAME_EN")
    private String PSG_NAME_EN;
    @JsonProperty("CERT_TYPE")
    private String CERT_TYPE;
    @JsonProperty("CERT_NUM")
    private String CERT_NUM;
    @JsonProperty("PASSENGER_TYPE")
    private String PASSENGER_TYPE;
    @JsonProperty("CONTACT_NAME")
    private String CONTACT_NAME;
    @JsonProperty("CONTACT_NO")
    private String CONTACT_NO;
    @JsonProperty("FLIGHT_ID")
    private Long FLIGHT_ID;
    @JsonProperty("TICKET_NUM")
    private String TICKET_NUM;
    @JsonProperty("PNR_NO")
    private String PNR_NO;
    @JsonProperty("TICKET_PRICE")
    private String TICKET_PRICE;
    @JsonProperty("TICKET_STATUS")
    private String TICKET_STATUS;
    @JsonProperty("TICKET_TIME")
    private Long TICKET_TIME;
    @JsonProperty("AIRPORT_TAX")
    private String AIRPORT_TAX;
    @JsonProperty("FUEL_TAX")
    private String FUEL_TAX;
    @JsonProperty("INSURE_TAX")
    private String INSURE_TAX;
    @JsonProperty("INSURE_STATUS")
    private String INSURE_STATUS;
    @JsonProperty("LY_VALUE")
    private String LY_VALUE;
    @JsonProperty("LY_STATUS")
    private String LY_STATUS;
    @JsonProperty("ACCOMPANY_NAME")
    private String ACCOMPANY_NAME;
    @JsonProperty("ACCOMPANY_CERT_NO")
    private String ACCOMPANY_CERT_NO;
    @JsonProperty("ACCOMPANY_TICKET_NUM")
    private String ACCOMPANY_TICKET_NUM;
    @JsonProperty("IS_ACCOMPANY")
    private String IS_ACCOMPANY;
    @JsonProperty("OLD_PSG_ID")
    private Long OLD_PSG_ID;
    @JsonProperty("AGE")
    private Integer AGE;
    @JsonProperty("CHANGE_FEE")
    private String CHANGE_FEE;
    @JsonProperty("COMPENSATION_FEE")
    private String COMPENSATION_FEE;
    @JsonProperty("LAST_CHANGE_FEE")
    private String LAST_CHANGE_FEE;
    @JsonProperty("REFUND_NUM")
    private String REFUND_NUM;
    @JsonProperty("REFUND_NUM_STATUS")
    private String REFUND_NUM_STATUS;
    @JsonProperty("REFUND_AMOUNT_STATUS")
    private String REFUND_AMOUNT_STATUS;
    @JsonProperty("INSURE_NO")
    private String INSURE_NO;
    @JsonProperty("INSURE_SERIAL_NUMBER")
    private String INSURE_SERIAL_NUMBER;
    @JsonProperty("OPT_STATUS")
    private String OPT_STATUS;
    @JsonProperty("CASH")
    private String CASH;
    @JsonProperty("TICKET_FAIL_REASON")
    private String TICKET_FAIL_REASON;
    @JsonProperty("INSURANCE_FAIL_REASON")
    private String INSURANCE_FAIL_REASON;
    @JsonProperty("STOP_AIRPORT")
    private String STOP_AIRPORT;
    @JsonProperty("STOP_TERMINAL")
    private String STOP_TERMINAL;
    @JsonProperty("CABIN_TYPE_NAME")
    private String CABIN_TYPE_NAME;
    @JsonProperty("MEAL")
    private String MEAL;

    @Override
    public String toString() {
        return "TOdsLyxOrderPassengerInfo{" +
                "ID=" + ID +
                ", PSG_NAME_CN='" + PSG_NAME_CN + '\'' +
                ", PSG_NAME_EN='" + PSG_NAME_EN + '\'' +
                ", CERT_TYPE='" + CERT_TYPE + '\'' +
                ", CERT_NUM='" + CERT_NUM + '\'' +
                ", PASSENGER_TYPE='" + PASSENGER_TYPE + '\'' +
                ", CONTACT_NAME='" + CONTACT_NAME + '\'' +
                ", CONTACT_NO='" + CONTACT_NO + '\'' +
                ", FLIGHT_ID=" + FLIGHT_ID +
                ", TICKET_NUM='" + TICKET_NUM + '\'' +
                ", PNR_NO='" + PNR_NO + '\'' +
                ", TICKET_PRICE='" + TICKET_PRICE + '\'' +
                ", TICKET_STATUS='" + TICKET_STATUS + '\'' +
                ", TICKET_TIME=" + TICKET_TIME +
                ", AIRPORT_TAX='" + AIRPORT_TAX + '\'' +
                ", FUEL_TAX='" + FUEL_TAX + '\'' +
                ", INSURE_TAX='" + INSURE_TAX + '\'' +
                ", INSURE_STATUS='" + INSURE_STATUS + '\'' +
                ", LY_VALUE='" + LY_VALUE + '\'' +
                ", LY_STATUS='" + LY_STATUS + '\'' +
                ", ACCOMPANY_NAME='" + ACCOMPANY_NAME + '\'' +
                ", ACCOMPANY_CERT_NO='" + ACCOMPANY_CERT_NO + '\'' +
                ", ACCOMPANY_TICKET_NUM='" + ACCOMPANY_TICKET_NUM + '\'' +
                ", IS_ACCOMPANY='" + IS_ACCOMPANY + '\'' +
                ", OLD_PSG_ID=" + OLD_PSG_ID +
                ", AGE=" + AGE +
                ", CHANGE_FEE='" + CHANGE_FEE + '\'' +
                ", COMPENSATION_FEE='" + COMPENSATION_FEE + '\'' +
                ", LAST_CHANGE_FEE='" + LAST_CHANGE_FEE + '\'' +
                ", REFUND_NUM='" + REFUND_NUM + '\'' +
                ", REFUND_NUM_STATUS='" + REFUND_NUM_STATUS + '\'' +
                ", REFUND_AMOUNT_STATUS='" + REFUND_AMOUNT_STATUS + '\'' +
                ", INSURE_NO='" + INSURE_NO + '\'' +
                ", INSURE_SERIAL_NUMBER='" + INSURE_SERIAL_NUMBER + '\'' +
                ", OPT_STATUS='" + OPT_STATUS + '\'' +
                ", CASH='" + CASH + '\'' +
                ", TICKET_FAIL_REASON='" + TICKET_FAIL_REASON + '\'' +
                ", INSURANCE_FAIL_REASON='" + INSURANCE_FAIL_REASON + '\'' +
                ", STOP_AIRPORT='" + STOP_AIRPORT + '\'' +
                ", STOP_TERMINAL='" + STOP_TERMINAL + '\'' +
                ", CABIN_TYPE_NAME='" + CABIN_TYPE_NAME + '\'' +
                ", MEAL='" + MEAL + '\'' +
                '}';
    }

    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public String getPSG_NAME_CN() {
        return PSG_NAME_CN;
    }

    public void setPSG_NAME_CN(String PSG_NAME_CN) {
        this.PSG_NAME_CN = PSG_NAME_CN;
    }

    public String getPSG_NAME_EN() {
        return PSG_NAME_EN;
    }

    public void setPSG_NAME_EN(String PSG_NAME_EN) {
        this.PSG_NAME_EN = PSG_NAME_EN;
    }

    public String getCERT_TYPE() {
        return CERT_TYPE;
    }

    public void setCERT_TYPE(String CERT_TYPE) {
        this.CERT_TYPE = CERT_TYPE;
    }

    public String getCERT_NUM() {
        return CERT_NUM;
    }

    public void setCERT_NUM(String CERT_NUM) {
        this.CERT_NUM = CERT_NUM;
    }

    public String getPASSENGER_TYPE() {
        return PASSENGER_TYPE;
    }

    public void setPASSENGER_TYPE(String PASSENGER_TYPE) {
        this.PASSENGER_TYPE = PASSENGER_TYPE;
    }

    public String getCONTACT_NAME() {
        return CONTACT_NAME;
    }

    public void setCONTACT_NAME(String CONTACT_NAME) {
        this.CONTACT_NAME = CONTACT_NAME;
    }

    public String getCONTACT_NO() {
        return CONTACT_NO;
    }

    public void setCONTACT_NO(String CONTACT_NO) {
        this.CONTACT_NO = CONTACT_NO;
    }

    public Long getFLIGHT_ID() {
        return FLIGHT_ID;
    }

    public void setFLIGHT_ID(Long FLIGHT_ID) {
        this.FLIGHT_ID = FLIGHT_ID;
    }

    public String getTICKET_NUM() {
        return TICKET_NUM;
    }

    public void setTICKET_NUM(String TICKET_NUM) {
        this.TICKET_NUM = TICKET_NUM;
    }

    public String getPNR_NO() {
        return PNR_NO;
    }

    public void setPNR_NO(String PNR_NO) {
        this.PNR_NO = PNR_NO;
    }

    public String getTICKET_PRICE() {
        return TICKET_PRICE;
    }

    public void setTICKET_PRICE(String TICKET_PRICE) {
        this.TICKET_PRICE = TICKET_PRICE;
    }

    public String getTICKET_STATUS() {
        return TICKET_STATUS;
    }

    public void setTICKET_STATUS(String TICKET_STATUS) {
        this.TICKET_STATUS = TICKET_STATUS;
    }

    public Long getTICKET_TIME() {
        return TICKET_TIME;
    }

    public void setTICKET_TIME(Long TICKET_TIME) {
        this.TICKET_TIME = TICKET_TIME;
    }

    public String getAIRPORT_TAX() {
        return AIRPORT_TAX;
    }

    public void setAIRPORT_TAX(String AIRPORT_TAX) {
        this.AIRPORT_TAX = AIRPORT_TAX;
    }

    public String getFUEL_TAX() {
        return FUEL_TAX;
    }

    public void setFUEL_TAX(String FUEL_TAX) {
        this.FUEL_TAX = FUEL_TAX;
    }

    public String getINSURE_TAX() {
        return INSURE_TAX;
    }

    public void setINSURE_TAX(String INSURE_TAX) {
        this.INSURE_TAX = INSURE_TAX;
    }

    public String getINSURE_STATUS() {
        return INSURE_STATUS;
    }

    public void setINSURE_STATUS(String INSURE_STATUS) {
        this.INSURE_STATUS = INSURE_STATUS;
    }

    public String getLY_VALUE() {
        return LY_VALUE;
    }

    public void setLY_VALUE(String LY_VALUE) {
        this.LY_VALUE = LY_VALUE;
    }

    public String getLY_STATUS() {
        return LY_STATUS;
    }

    public void setLY_STATUS(String LY_STATUS) {
        this.LY_STATUS = LY_STATUS;
    }

    public String getACCOMPANY_NAME() {
        return ACCOMPANY_NAME;
    }

    public void setACCOMPANY_NAME(String ACCOMPANY_NAME) {
        this.ACCOMPANY_NAME = ACCOMPANY_NAME;
    }

    public String getACCOMPANY_CERT_NO() {
        return ACCOMPANY_CERT_NO;
    }

    public void setACCOMPANY_CERT_NO(String ACCOMPANY_CERT_NO) {
        this.ACCOMPANY_CERT_NO = ACCOMPANY_CERT_NO;
    }

    public String getACCOMPANY_TICKET_NUM() {
        return ACCOMPANY_TICKET_NUM;
    }

    public void setACCOMPANY_TICKET_NUM(String ACCOMPANY_TICKET_NUM) {
        this.ACCOMPANY_TICKET_NUM = ACCOMPANY_TICKET_NUM;
    }

    public String getIS_ACCOMPANY() {
        return IS_ACCOMPANY;
    }

    public void setIS_ACCOMPANY(String IS_ACCOMPANY) {
        this.IS_ACCOMPANY = IS_ACCOMPANY;
    }

    public Long getOLD_PSG_ID() {
        return OLD_PSG_ID;
    }

    public void setOLD_PSG_ID(Long OLD_PSG_ID) {
        this.OLD_PSG_ID = OLD_PSG_ID;
    }

    public Integer getAGE() {
        return AGE;
    }

    public void setAGE(Integer AGE) {
        this.AGE = AGE;
    }

    public String getCHANGE_FEE() {
        return CHANGE_FEE;
    }

    public void setCHANGE_FEE(String CHANGE_FEE) {
        this.CHANGE_FEE = CHANGE_FEE;
    }

    public String getCOMPENSATION_FEE() {
        return COMPENSATION_FEE;
    }

    public void setCOMPENSATION_FEE(String COMPENSATION_FEE) {
        this.COMPENSATION_FEE = COMPENSATION_FEE;
    }

    public String getLAST_CHANGE_FEE() {
        return LAST_CHANGE_FEE;
    }

    public void setLAST_CHANGE_FEE(String LAST_CHANGE_FEE) {
        this.LAST_CHANGE_FEE = LAST_CHANGE_FEE;
    }

    public String getREFUND_NUM() {
        return REFUND_NUM;
    }

    public void setREFUND_NUM(String REFUND_NUM) {
        this.REFUND_NUM = REFUND_NUM;
    }

    public String getREFUND_NUM_STATUS() {
        return REFUND_NUM_STATUS;
    }

    public void setREFUND_NUM_STATUS(String REFUND_NUM_STATUS) {
        this.REFUND_NUM_STATUS = REFUND_NUM_STATUS;
    }

    public String getREFUND_AMOUNT_STATUS() {
        return REFUND_AMOUNT_STATUS;
    }

    public void setREFUND_AMOUNT_STATUS(String REFUND_AMOUNT_STATUS) {
        this.REFUND_AMOUNT_STATUS = REFUND_AMOUNT_STATUS;
    }

    public String getINSURE_NO() {
        return INSURE_NO;
    }

    public void setINSURE_NO(String INSURE_NO) {
        this.INSURE_NO = INSURE_NO;
    }

    public String getINSURE_SERIAL_NUMBER() {
        return INSURE_SERIAL_NUMBER;
    }

    public void setINSURE_SERIAL_NUMBER(String INSURE_SERIAL_NUMBER) {
        this.INSURE_SERIAL_NUMBER = INSURE_SERIAL_NUMBER;
    }

    public String getOPT_STATUS() {
        return OPT_STATUS;
    }

    public void setOPT_STATUS(String OPT_STATUS) {
        this.OPT_STATUS = OPT_STATUS;
    }

    public String getCASH() {
        return CASH;
    }

    public void setCASH(String CASH) {
        this.CASH = CASH;
    }

    public String getTICKET_FAIL_REASON() {
        return TICKET_FAIL_REASON;
    }

    public void setTICKET_FAIL_REASON(String TICKET_FAIL_REASON) {
        this.TICKET_FAIL_REASON = TICKET_FAIL_REASON;
    }

    public String getINSURANCE_FAIL_REASON() {
        return INSURANCE_FAIL_REASON;
    }

    public void setINSURANCE_FAIL_REASON(String INSURANCE_FAIL_REASON) {
        this.INSURANCE_FAIL_REASON = INSURANCE_FAIL_REASON;
    }

    public String getSTOP_AIRPORT() {
        return STOP_AIRPORT;
    }

    public void setSTOP_AIRPORT(String STOP_AIRPORT) {
        this.STOP_AIRPORT = STOP_AIRPORT;
    }

    public String getSTOP_TERMINAL() {
        return STOP_TERMINAL;
    }

    public void setSTOP_TERMINAL(String STOP_TERMINAL) {
        this.STOP_TERMINAL = STOP_TERMINAL;
    }

    public String getCABIN_TYPE_NAME() {
        return CABIN_TYPE_NAME;
    }

    public void setCABIN_TYPE_NAME(String CABIN_TYPE_NAME) {
        this.CABIN_TYPE_NAME = CABIN_TYPE_NAME;
    }

    public String getMEAL() {
        return MEAL;
    }

    public void setMEAL(String MEAL) {
        this.MEAL = MEAL;
    }
}
