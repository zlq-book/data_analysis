package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

/**
 * @author kuangaihua
 * @date 2025/7/8 16:59
 */
public class TOdsHytdTManageOthercard {
    @JsonProperty("ETL_DATE")
    private Date ETL_DATE;
    @JsonProperty("ID")
    private String ID;
    @JsonProperty("CRM_CARDNO")
    private String CRM_CARDNO;
    @JsonProperty("CARD_NO")
    private String CARD_NO;
    @JsonProperty("NAME")
    private String NAME;
    @JsonProperty("NAME_EN")
    private String NAME_EN;
    @JsonProperty("CERT_TYPE")
    private String CERT_TYPE;
    @JsonProperty("CERT_NO")
    private String CERT_NO;
    @JsonProperty("END_TIME")
    private Long END_TIME;

    public TOdsHytdTManageOthercard() {
    }

    @Override
    public String toString() {
        return "TOdsHytdTManageOthercard{" +
                "ETL_DATE=" + ETL_DATE +
                ", ID='" + ID + '\'' +
                ", CRM_CARDNO='" + CRM_CARDNO + '\'' +
                ", CARD_NO='" + CARD_NO + '\'' +
                ", NAME='" + NAME + '\'' +
                ", NAME_EN='" + NAME_EN + '\'' +
                ", CERT_TYPE='" + CERT_TYPE + '\'' +
                ", CERT_NO='" + CERT_NO + '\'' +
                ", END_TIME=" + END_TIME +
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

    public String getCRM_CARDNO() {
        return CRM_CARDNO;
    }

    public void setCRM_CARDNO(String CRM_CARDNO) {
        this.CRM_CARDNO = CRM_CARDNO;
    }

    public String getCARD_NO() {
        return CARD_NO;
    }

    public void setCARD_NO(String CARD_NO) {
        this.CARD_NO = CARD_NO;
    }

    public String getNAME() {
        return NAME;
    }

    public void setNAME(String NAME) {
        this.NAME = NAME;
    }

    public String getNAME_EN() {
        return NAME_EN;
    }

    public void setNAME_EN(String NAME_EN) {
        this.NAME_EN = NAME_EN;
    }

    public String getCERT_TYPE() {
        return CERT_TYPE;
    }

    public void setCERT_TYPE(String CERT_TYPE) {
        this.CERT_TYPE = CERT_TYPE;
    }

    public String getCERT_NO() {
        return CERT_NO;
    }

    public void setCERT_NO(String CERT_NO) {
        this.CERT_NO = CERT_NO;
    }

    public Long getEND_TIME() {
        return END_TIME;
    }

    public void setEND_TIME(Long END_TIME) {
        this.END_TIME = END_TIME;
    }
}
