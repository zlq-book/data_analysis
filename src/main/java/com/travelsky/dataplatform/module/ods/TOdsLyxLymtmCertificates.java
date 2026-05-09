package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TOdsLyxLymtmCertificates {
    @JsonProperty("ID")
    private Long ID;
    @JsonProperty("MTM_CARD_NUM")
    private String MTM_CARD_NUM;
    @JsonProperty("ID_TYPE")
    private String ID_TYPE;
    @JsonProperty("ID_NUM")
    private String ID_NUM;
    @JsonProperty("MTM_ID_FLG")
    private String MTM_ID_FLG;
    @JsonProperty("ID_STATUS")
    private String ID_STATUS;
    @JsonProperty("EFFECTIVE_TIME")
    private Long EFFECTIVE_TIME;
    @JsonProperty("EXPIRES_TIME")
    private Long EXPIRES_TIME;
    @JsonProperty("DOC_PHOTO1")
    private String DOC_PHOTO1;
    @JsonProperty("DOC_PHOTO2")
    private String DOC_PHOTO2;
    @JsonProperty("DOC_PHOTO3")
    private String DOC_PHOTO3;
    @JsonProperty("DOC_PHOTO4")
    private String DOC_PHOTO4;
    @JsonProperty("DOC_PHOTO5")
    private String DOC_PHOTO5;
    @JsonProperty("EXPIRES_OPERATOR")
    private String EXPIRES_OPERATOR;
    @JsonProperty("VERSION")
    private Long VERSION;
    @JsonProperty("DEL_FLG")
    private String DEL_FLG;
    @JsonProperty("CREATE_ID")
    private String CREATE_ID;
    @JsonProperty("CREATE_TIME")
    private Long CREATE_TIME;
    @JsonProperty("UPDATE_ID")
    private String UPDATE_ID;
    @JsonProperty("UPDATE_TIME")
    private Long UPDATE_TIME;
    @JsonProperty("ID_NUM_LYGJ")
    private String ID_NUM_LYGJ;

    @Override
    public String toString() {
        return "TOdsLyxLymtmCertificates{" +
                "ID=" + ID +
                ", MTM_CARD_NUM='" + MTM_CARD_NUM + '\'' +
                ", ID_TYPE='" + ID_TYPE + '\'' +
                ", ID_NUM='" + ID_NUM + '\'' +
                ", MTM_ID_FLG='" + MTM_ID_FLG + '\'' +
                ", ID_STATUS='" + ID_STATUS + '\'' +
                ", EFFECTIVE_TIME=" + EFFECTIVE_TIME +
                ", EXPIRES_TIME=" + EXPIRES_TIME +
                ", DOC_PHOTO1='" + DOC_PHOTO1 + '\'' +
                ", DOC_PHOTO2='" + DOC_PHOTO2 + '\'' +
                ", DOC_PHOTO3='" + DOC_PHOTO3 + '\'' +
                ", DOC_PHOTO4='" + DOC_PHOTO4 + '\'' +
                ", DOC_PHOTO5='" + DOC_PHOTO5 + '\'' +
                ", EXPIRES_OPERATOR='" + EXPIRES_OPERATOR + '\'' +
                ", VERSION=" + VERSION +
                ", DEL_FLG='" + DEL_FLG + '\'' +
                ", CREATE_ID='" + CREATE_ID + '\'' +
                ", CREATE_TIME=" + CREATE_TIME +
                ", UPDATE_ID='" + UPDATE_ID + '\'' +
                ", UPDATE_TIME=" + UPDATE_TIME +
                ", ID_NUM_LYGJ='" + ID_NUM_LYGJ + '\'' +
                '}';
    }

    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public String getMTM_CARD_NUM() {
        return MTM_CARD_NUM;
    }

    public void setMTM_CARD_NUM(String MTM_CARD_NUM) {
        this.MTM_CARD_NUM = MTM_CARD_NUM;
    }

    public String getID_TYPE() {
        return ID_TYPE;
    }

    public void setID_TYPE(String ID_TYPE) {
        this.ID_TYPE = ID_TYPE;
    }

    public String getID_NUM() {
        return ID_NUM;
    }

    public void setID_NUM(String ID_NUM) {
        this.ID_NUM = ID_NUM;
    }

    public String getMTM_ID_FLG() {
        return MTM_ID_FLG;
    }

    public void setMTM_ID_FLG(String MTM_ID_FLG) {
        this.MTM_ID_FLG = MTM_ID_FLG;
    }

    public String getID_STATUS() {
        return ID_STATUS;
    }

    public void setID_STATUS(String ID_STATUS) {
        this.ID_STATUS = ID_STATUS;
    }

    public Long getEFFECTIVE_TIME() {
        return EFFECTIVE_TIME;
    }

    public void setEFFECTIVE_TIME(Long EFFECTIVE_TIME) {
        this.EFFECTIVE_TIME = EFFECTIVE_TIME;
    }

    public Long getEXPIRES_TIME() {
        return EXPIRES_TIME;
    }

    public void setEXPIRES_TIME(Long EXPIRES_TIME) {
        this.EXPIRES_TIME = EXPIRES_TIME;
    }

    public String getDOC_PHOTO1() {
        return DOC_PHOTO1;
    }

    public void setDOC_PHOTO1(String DOC_PHOTO1) {
        this.DOC_PHOTO1 = DOC_PHOTO1;
    }

    public String getDOC_PHOTO2() {
        return DOC_PHOTO2;
    }

    public void setDOC_PHOTO2(String DOC_PHOTO2) {
        this.DOC_PHOTO2 = DOC_PHOTO2;
    }

    public String getDOC_PHOTO3() {
        return DOC_PHOTO3;
    }

    public void setDOC_PHOTO3(String DOC_PHOTO3) {
        this.DOC_PHOTO3 = DOC_PHOTO3;
    }

    public String getDOC_PHOTO4() {
        return DOC_PHOTO4;
    }

    public void setDOC_PHOTO4(String DOC_PHOTO4) {
        this.DOC_PHOTO4 = DOC_PHOTO4;
    }

    public String getDOC_PHOTO5() {
        return DOC_PHOTO5;
    }

    public void setDOC_PHOTO5(String DOC_PHOTO5) {
        this.DOC_PHOTO5 = DOC_PHOTO5;
    }

    public String getEXPIRES_OPERATOR() {
        return EXPIRES_OPERATOR;
    }

    public void setEXPIRES_OPERATOR(String EXPIRES_OPERATOR) {
        this.EXPIRES_OPERATOR = EXPIRES_OPERATOR;
    }

    public Long getVERSION() {
        return VERSION;
    }

    public void setVERSION(Long VERSION) {
        this.VERSION = VERSION;
    }

    public String getDEL_FLG() {
        return DEL_FLG;
    }

    public void setDEL_FLG(String DEL_FLG) {
        this.DEL_FLG = DEL_FLG;
    }

    public String getCREATE_ID() {
        return CREATE_ID;
    }

    public void setCREATE_ID(String CREATE_ID) {
        this.CREATE_ID = CREATE_ID;
    }

    public Long getCREATE_TIME() {
        return CREATE_TIME;
    }

    public void setCREATE_TIME(Long CREATE_TIME) {
        this.CREATE_TIME = CREATE_TIME;
    }

    public String getUPDATE_ID() {
        return UPDATE_ID;
    }

    public void setUPDATE_ID(String UPDATE_ID) {
        this.UPDATE_ID = UPDATE_ID;
    }

    public Long getUPDATE_TIME() {
        return UPDATE_TIME;
    }

    public void setUPDATE_TIME(Long UPDATE_TIME) {
        this.UPDATE_TIME = UPDATE_TIME;
    }

    public String getID_NUM_LYGJ() {
        return ID_NUM_LYGJ;
    }

    public void setID_NUM_LYGJ(String ID_NUM_LYGJ) {
        this.ID_NUM_LYGJ = ID_NUM_LYGJ;
    }
}
