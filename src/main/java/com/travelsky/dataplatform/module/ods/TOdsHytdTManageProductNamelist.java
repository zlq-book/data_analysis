package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;
import java.util.Date;

/**
 * @author kuangaihua
 * @date 2025/7/8 16:59
 */
public class TOdsHytdTManageProductNamelist {
    @JsonProperty("ETL_DATE")
    private Date ETL_DATE;

    @JsonProperty("ID")
    private String ID;

    @JsonProperty("PRODUCT_ID")
    private String PRODUCT_ID;

    @JsonProperty("MEMBER_ID")
    private String MEMBER_ID;

    @JsonProperty("CREATED_TIME")
    private Long CREATED_TIME;

    @JsonProperty("MODIFIED_TIME")
    private Long MODIFIED_TIME;

    @JsonProperty("MEMBER_CARD")
    private String MEMBER_CARD;

    @JsonProperty("CRED_CODE")
    private String CRED_CODE;

    @JsonProperty("MOBILE_NUMBER")
    private String MOBILE_NUMBER;

    @JsonProperty("ATTRIBUTE")
    private String ATTRIBUTE;

    @JsonProperty("LEVEL_NAME")
    private String LEVEL_NAME;

    @JsonProperty("CN_LAST_NAME")
    private String CN_LAST_NAME;

    @JsonProperty("CN_FIRST_NAME")
    private String CN_FIRST_NAME;

    @JsonProperty("LAST_NAME")
    private String LAST_NAME;

    @JsonProperty("FIRST_NAME")
    private String FIRST_NAME;

    @JsonProperty("DATEOF_BIRTH")
    private Long DATEOF_BIRTH;

    @JsonProperty("PROMO_CODE")
    private String PROMO_CODE;

    @JsonProperty("CHANNEL_ID")
    private String CHANNEL_ID;

    @JsonProperty("CHANNEL_NAME")
    private String CHANNEL_NAME;

    public TOdsHytdTManageProductNamelist() {
    }

    @Override
    public String toString() {
        return "TOdsHytdTManageProductNamelist{" +
                "ETL_DATE=" + ETL_DATE +
                ", ID='" + ID + '\'' +
                ", PRODUCT_ID='" + PRODUCT_ID + '\'' +
                ", MEMBER_ID='" + MEMBER_ID + '\'' +
                ", CREATED_TIME=" + CREATED_TIME +
                ", MODIFIED_TIME=" + MODIFIED_TIME +
                ", MEMBER_CARD='" + MEMBER_CARD + '\'' +
                ", CRED_CODE='" + CRED_CODE + '\'' +
                ", MOBILE_NUMBER='" + MOBILE_NUMBER + '\'' +
                ", ATTRIBUTE='" + ATTRIBUTE + '\'' +
                ", LEVEL_NAME='" + LEVEL_NAME + '\'' +
                ", CN_LAST_NAME='" + CN_LAST_NAME + '\'' +
                ", CN_FIRST_NAME='" + CN_FIRST_NAME + '\'' +
                ", LAST_NAME='" + LAST_NAME + '\'' +
                ", FIRST_NAME='" + FIRST_NAME + '\'' +
                ", DATEOF_BIRTH=" + DATEOF_BIRTH +
                ", PROMO_CODE='" + PROMO_CODE + '\'' +
                ", CHANNEL_ID='" + CHANNEL_ID + '\'' +
                ", CHANNEL_NAME='" + CHANNEL_NAME + '\'' +
                '}';
    }

    public Date getETL_DATE() {
        return ETL_DATE;
    }

    public void setETL_DATE(Date ETL_DATE) {
        this.ETL_DATE = ETL_DATE;
    }

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public String getPRODUCT_ID() {
        return PRODUCT_ID;
    }

    public void setPRODUCT_ID(String PRODUCT_ID) {
        this.PRODUCT_ID = PRODUCT_ID;
    }

    public String getMEMBER_ID() {
        return MEMBER_ID;
    }

    public void setMEMBER_ID(String MEMBER_ID) {
        this.MEMBER_ID = MEMBER_ID;
    }

    public Long getCREATED_TIME() {
        return CREATED_TIME;
    }

    public void setCREATED_TIME(Long CREATED_TIME) {
        this.CREATED_TIME = CREATED_TIME;
    }

    public Long getMODIFIED_TIME() {
        return MODIFIED_TIME;
    }

    public void setMODIFIED_TIME(Long MODIFIED_TIME) {
        this.MODIFIED_TIME = MODIFIED_TIME;
    }

    public String getMEMBER_CARD() {
        return MEMBER_CARD;
    }

    public void setMEMBER_CARD(String MEMBER_CARD) {
        this.MEMBER_CARD = MEMBER_CARD;
    }

    public String getCRED_CODE() {
        return CRED_CODE;
    }

    public void setCRED_CODE(String CRED_CODE) {
        this.CRED_CODE = CRED_CODE;
    }

    public String getMOBILE_NUMBER() {
        return MOBILE_NUMBER;
    }

    public void setMOBILE_NUMBER(String MOBILE_NUMBER) {
        this.MOBILE_NUMBER = MOBILE_NUMBER;
    }

    public String getATTRIBUTE() {
        return ATTRIBUTE;
    }

    public void setATTRIBUTE(String ATTRIBUTE) {
        this.ATTRIBUTE = ATTRIBUTE;
    }

    public String getLEVEL_NAME() {
        return LEVEL_NAME;
    }

    public void setLEVEL_NAME(String LEVEL_NAME) {
        this.LEVEL_NAME = LEVEL_NAME;
    }

    public String getCN_LAST_NAME() {
        return CN_LAST_NAME;
    }

    public void setCN_LAST_NAME(String CN_LAST_NAME) {
        this.CN_LAST_NAME = CN_LAST_NAME;
    }

    public String getCN_FIRST_NAME() {
        return CN_FIRST_NAME;
    }

    public void setCN_FIRST_NAME(String CN_FIRST_NAME) {
        this.CN_FIRST_NAME = CN_FIRST_NAME;
    }

    public String getLAST_NAME() {
        return LAST_NAME;
    }

    public void setLAST_NAME(String LAST_NAME) {
        this.LAST_NAME = LAST_NAME;
    }

    public String getFIRST_NAME() {
        return FIRST_NAME;
    }

    public void setFIRST_NAME(String FIRST_NAME) {
        this.FIRST_NAME = FIRST_NAME;
    }

    public Long getDATEOF_BIRTH() {
        return DATEOF_BIRTH;
    }

    public void setDATEOF_BIRTH(Long DATEOF_BIRTH) {
        this.DATEOF_BIRTH = DATEOF_BIRTH;
    }

    public String getPROMO_CODE() {
        return PROMO_CODE;
    }

    public void setPROMO_CODE(String PROMO_CODE) {
        this.PROMO_CODE = PROMO_CODE;
    }

    public String getCHANNEL_ID() {
        return CHANNEL_ID;
    }

    public void setCHANNEL_ID(String CHANNEL_ID) {
        this.CHANNEL_ID = CHANNEL_ID;
    }

    public String getCHANNEL_NAME() {
        return CHANNEL_NAME;
    }

    public void setCHANNEL_NAME(String CHANNEL_NAME) {
        this.CHANNEL_NAME = CHANNEL_NAME;
    }
}
