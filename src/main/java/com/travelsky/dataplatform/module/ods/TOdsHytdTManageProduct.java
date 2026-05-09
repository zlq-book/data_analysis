package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * SCPHOENIX.T_MANAGE_PRODUCT表对应的实体类
 * 产品管理信息表
 * @author
 * @date 2025/10/14
 */
public class TOdsHytdTManageProduct {
    @JsonProperty("ID")
    private String ID;

    @JsonProperty("NAME")
    private String NAME;

    @JsonProperty("START_TIME")
    private Long START_TIME;

    @JsonProperty("END_TIME")
    private Long END_TIME;

    @JsonProperty("STATUS")
    private String STATUS;

    @JsonProperty("IMG_URL")
    private String IMG_URL;

    @JsonProperty("CONTENT")
    private String CONTENT;

    @JsonProperty("CREATED_TIME")
    private Long CREATED_TIME;

    @JsonProperty("MODIFIED_TIME")
    private Long MODIFIED_TIME;

    @JsonProperty("PROMO_CODE")
    private String PROMO_CODE;

    @JsonProperty("PAGE_TYPE")
    private String PAGE_TYPE;

    @JsonProperty("PAGE_URL")
    private String PAGE_URL;

    @JsonProperty("PAGE_URL_PC")
    private String PAGE_URL_PC;

    @JsonProperty("IMG_URL_HEIGHT")
    private String IMG_URL_HEIGHT;

    @JsonProperty("IMG_URL_LIST")
    private String IMG_URL_LIST;

    @JsonProperty("IMG_URL_PC")
    private String IMG_URL_PC;

    @JsonProperty("AIR_CHINA_END_TIME")
    private Long AIR_CHINA_END_TIME;

    @JsonProperty("CLICK_VOLUME")
    private Integer CLICK_VOLUME;

    @JsonProperty("CONTENT_PC")
    private String CONTENT_PC;

    @JsonProperty("NEED_SIGN")
    private String NEED_SIGN;

    @Override
    public String toString() {
        return "TOdsHytdTManageProduct{" +
                "ID='" + ID + '\'' +
                ", NAME='" + NAME + '\'' +
                ", START_TIME=" + START_TIME +
                ", END_TIME=" + END_TIME +
                ", STATUS='" + STATUS + '\'' +
                ", IMG_URL='" + IMG_URL + '\'' +
                ", CONTENT='" + CONTENT + '\'' +
                ", CREATED_TIME=" + CREATED_TIME +
                ", MODIFIED_TIME=" + MODIFIED_TIME +
                ", PROMO_CODE='" + PROMO_CODE + '\'' +
                ", PAGE_TYPE='" + PAGE_TYPE + '\'' +
                ", PAGE_URL='" + PAGE_URL + '\'' +
                ", PAGE_URL_PC='" + PAGE_URL_PC + '\'' +
                ", IMG_URL_HEIGHT='" + IMG_URL_HEIGHT + '\'' +
                ", IMG_URL_LIST='" + IMG_URL_LIST + '\'' +
                ", IMG_URL_PC='" + IMG_URL_PC + '\'' +
                ", AIR_CHINA_END_TIME=" + AIR_CHINA_END_TIME +
                ", CLICK_VOLUME=" + CLICK_VOLUME +
                ", CONTENT_PC='" + CONTENT_PC + '\'' +
                ", NEED_SIGN='" + NEED_SIGN + '\'' +
                '}';
    }

    // Getter和Setter方法
    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public String getNAME() {
        return NAME;
    }

    public void setNAME(String NAME) {
        this.NAME = NAME;
    }

    public Long getSTART_TIME() {
        return START_TIME;
    }

    public void setSTART_TIME(Long START_TIME) {
        this.START_TIME = START_TIME;
    }

    public Long getEND_TIME() {
        return END_TIME;
    }

    public void setEND_TIME(Long END_TIME) {
        this.END_TIME = END_TIME;
    }

    public String getSTATUS() {
        return STATUS;
    }

    public void setSTATUS(String STATUS) {
        this.STATUS = STATUS;
    }

    public String getIMG_URL() {
        return IMG_URL;
    }

    public void setIMG_URL(String IMG_URL) {
        this.IMG_URL = IMG_URL;
    }

    public String getCONTENT() {
        return CONTENT;
    }

    public void setCONTENT(String CONTENT) {
        this.CONTENT = CONTENT;
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

    public String getPROMO_CODE() {
        return PROMO_CODE;
    }

    public void setPROMO_CODE(String PROMO_CODE) {
        this.PROMO_CODE = PROMO_CODE;
    }

    public String getPAGE_TYPE() {
        return PAGE_TYPE;
    }

    public void setPAGE_TYPE(String PAGE_TYPE) {
        this.PAGE_TYPE = PAGE_TYPE;
    }

    public String getPAGE_URL() {
        return PAGE_URL;
    }

    public void setPAGE_URL(String PAGE_URL) {
        this.PAGE_URL = PAGE_URL;
    }

    public String getPAGE_URL_PC() {
        return PAGE_URL_PC;
    }

    public void setPAGE_URL_PC(String PAGE_URL_PC) {
        this.PAGE_URL_PC = PAGE_URL_PC;
    }

    public String getIMG_URL_HEIGHT() {
        return IMG_URL_HEIGHT;
    }

    public void setIMG_URL_HEIGHT(String IMG_URL_HEIGHT) {
        this.IMG_URL_HEIGHT = IMG_URL_HEIGHT;
    }

    public String getIMG_URL_LIST() {
        return IMG_URL_LIST;
    }

    public void setIMG_URL_LIST(String IMG_URL_LIST) {
        this.IMG_URL_LIST = IMG_URL_LIST;
    }

    public String getIMG_URL_PC() {
        return IMG_URL_PC;
    }

    public void setIMG_URL_PC(String IMG_URL_PC) {
        this.IMG_URL_PC = IMG_URL_PC;
    }

    public Long getAIR_CHINA_END_TIME() {
        return AIR_CHINA_END_TIME;
    }

    public void setAIR_CHINA_END_TIME(Long AIR_CHINA_END_TIME) {
        this.AIR_CHINA_END_TIME = AIR_CHINA_END_TIME;
    }

    public Integer getCLICK_VOLUME() {
        return CLICK_VOLUME;
    }

    public void setCLICK_VOLUME(Integer CLICK_VOLUME) {
        this.CLICK_VOLUME = CLICK_VOLUME;
    }

    public String getCONTENT_PC() {
        return CONTENT_PC;
    }

    public void setCONTENT_PC(String CONTENT_PC) {
        this.CONTENT_PC = CONTENT_PC;
    }

    public String getNEED_SIGN() {
        return NEED_SIGN;
    }

    public void setNEED_SIGN(String NEED_SIGN) {
        this.NEED_SIGN = NEED_SIGN;
    }
}



