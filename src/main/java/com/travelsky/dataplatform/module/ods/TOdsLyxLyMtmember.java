package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TOdsLyxLyMtmember {
    @JsonProperty("ID")
    private Long ID;
    @JsonProperty("MTM_CARD_NUM")
    private String MTM_CARD_NUM;
    @JsonProperty("MTM_STATUS")
    private String MTM_STATUS;
    @JsonProperty("MTM_PASS")
    private String MTM_PASS;
    @JsonProperty("MTM_SURNAME_CN")
    private String MTM_SURNAME_CN;
    @JsonProperty("MTM_NAME_CN")
    private String MTM_NAME_CN;
    @JsonProperty("MTM_SURNAME_EN")
    private String MTM_SURNAME_EN;
    @JsonProperty("MTM_NAME_EN")
    private String MTM_NAME_EN;
    @JsonProperty("MTM_HIPPOCRATES")
    private String MTM_HIPPOCRATES;
    @JsonProperty("MTM_NATIONALITY")
    private String MTM_NATIONALITY;
    @JsonProperty("MTM_BIRTHDAY")
    private Long MTM_BIRTHDAY;
    @JsonProperty("MTM_REGISTER_TIME")
    private Long MTM_REGISTER_TIME;
    @JsonProperty("PASS_ACTIVE_STATE")
    private String PASS_ACTIVE_STATE;
    @JsonProperty("REALNAME_ATTESTATION")
    private String REALNAME_ATTESTATION;
    @JsonProperty("ATTESTATION_MODE")
    private String ATTESTATION_MODE;
    @JsonProperty("ATTESTATION_TIME")
    private Long ATTESTATION_TIME;
    @JsonProperty("MTM_MOBILE")
    private String MTM_MOBILE;
    @JsonProperty("MAIN_ID_TYPE")
    private String MAIN_ID_TYPE;
    @JsonProperty("MAIN_ID_NUM")
    private String MAIN_ID_NUM;
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
    @JsonProperty("LINK_ID")
    private String LINK_ID;
    @JsonProperty("LINK_SOURCE")
    private String LINK_SOURCE;
    @JsonProperty("GESTURE_STATUS")
    private String GESTURE_STATUS;
    @JsonProperty("GESTURE_PASS")
    private String GESTURE_PASS;
    @JsonProperty("GESTURE_TIP")
    private String GESTURE_TIP;
    @JsonProperty("TIMING_TASK_STATUS")
    private String TIMING_TASK_STATUS;
    @JsonProperty("LIP_ATTESTATION")
    private String LIP_ATTESTATION;
    @JsonProperty("REGISTER_STATUS")
    private String REGISTER_STATUS;
    @JsonProperty("PRIVATE_POLICY_STATUS")
    private String PRIVATE_POLICY_STATUS;
    @JsonProperty("PRIVATE_POLICY_SEND_TIME")
    private Long PRIVATE_POLICY_SEND_TIME;
    @JsonProperty("PRIVATE_POLICY_CONFIRM_TIME")
    private Long PRIVATE_POLICY_CONFIRM_TIME;
    @JsonProperty("LEVEL_UPDATE_TIME")
    private Long LEVEL_UPDATE_TIME;
    @JsonProperty("ORI_LEVEL")
    private String ORI_LEVEL;
    @JsonProperty("UPPWD_DATE")
    private Long UPPWD_DATE;
    @JsonProperty("DIRECT_SELL_USER_ID")
    private String DIRECT_SELL_USER_ID;

    @Override
    public String toString() {
        return "TOdsLyxLyMtmember{" +
                "ID=" + ID +
                ", MTM_CARD_NUM='" + MTM_CARD_NUM + '\'' +
                ", MTM_STATUS='" + MTM_STATUS + '\'' +
                ", MTM_PASS='" + MTM_PASS + '\'' +
                ", MTM_SURNAME_CN='" + MTM_SURNAME_CN + '\'' +
                ", MTM_NAME_CN='" + MTM_NAME_CN + '\'' +
                ", MTM_SURNAME_EN='" + MTM_SURNAME_EN + '\'' +
                ", MTM_NAME_EN='" + MTM_NAME_EN + '\'' +
                ", MTM_HIPPOCRATES='" + MTM_HIPPOCRATES + '\'' +
                ", MTM_NATIONALITY='" + MTM_NATIONALITY + '\'' +
                ", MTM_BIRTHDAY=" + MTM_BIRTHDAY +
                ", MTM_REGISTER_TIME=" + MTM_REGISTER_TIME +
                ", PASS_ACTIVE_STATE='" + PASS_ACTIVE_STATE + '\'' +
                ", REALNAME_ATTESTATION='" + REALNAME_ATTESTATION + '\'' +
                ", ATTESTATION_MODE='" + ATTESTATION_MODE + '\'' +
                ", ATTESTATION_TIME='" + ATTESTATION_TIME + '\'' +
                ", MTM_MOBILE='" + MTM_MOBILE + '\'' +
                ", MAIN_ID_TYPE='" + MAIN_ID_TYPE + '\'' +
                ", MAIN_ID_NUM='" + MAIN_ID_NUM + '\'' +
                ", VERSION=" + VERSION +
                ", DEL_FLG='" + DEL_FLG + '\'' +
                ", CREATE_ID='" + CREATE_ID + '\'' +
                ", CREATE_TIME=" + CREATE_TIME +
                ", UPDATE_ID='" + UPDATE_ID + '\'' +
                ", UPDATE_TIME=" + UPDATE_TIME +
                ", LINK_ID='" + LINK_ID + '\'' +
                ", LINK_SOURCE='" + LINK_SOURCE + '\'' +
                ", GESTURE_STATUS='" + GESTURE_STATUS + '\'' +
                ", GESTURE_PASS='" + GESTURE_PASS + '\'' +
                ", GESTURE_TIP='" + GESTURE_TIP + '\'' +
                ", TIMING_TASK_STATUS='" + TIMING_TASK_STATUS + '\'' +
                ", LIP_ATTESTATION='" + LIP_ATTESTATION + '\'' +
                ", REGISTER_STATUS='" + REGISTER_STATUS + '\'' +
                ", PRIVATE_POLICY_STATUS='" + PRIVATE_POLICY_STATUS + '\'' +
                ", PRIVATE_POLICY_SEND_TIME=" + PRIVATE_POLICY_SEND_TIME +
                ", PRIVATE_POLICY_CONFIRM_TIME=" + PRIVATE_POLICY_CONFIRM_TIME +
                ", LEVEL_UPDATE_TIME=" + LEVEL_UPDATE_TIME +
                ", ORI_LEVEL='" + ORI_LEVEL + '\'' +
                ", UPPWD_DATE=" + UPPWD_DATE +
                ", DIRECT_SELL_USER_ID='" + DIRECT_SELL_USER_ID + '\'' +
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

    public String getMTM_STATUS() {
        return MTM_STATUS;
    }

    public void setMTM_STATUS(String MTM_STATUS) {
        this.MTM_STATUS = MTM_STATUS;
    }

    public String getMTM_PASS() {
        return MTM_PASS;
    }

    public void setMTM_PASS(String MTM_PASS) {
        this.MTM_PASS = MTM_PASS;
    }

    public String getMTM_SURNAME_CN() {
        return MTM_SURNAME_CN;
    }

    public void setMTM_SURNAME_CN(String MTM_SURNAME_CN) {
        this.MTM_SURNAME_CN = MTM_SURNAME_CN;
    }

    public String getMTM_NAME_CN() {
        return MTM_NAME_CN;
    }

    public void setMTM_NAME_CN(String MTM_NAME_CN) {
        this.MTM_NAME_CN = MTM_NAME_CN;
    }

    public String getMTM_SURNAME_EN() {
        return MTM_SURNAME_EN;
    }

    public void setMTM_SURNAME_EN(String MTM_SURNAME_EN) {
        this.MTM_SURNAME_EN = MTM_SURNAME_EN;
    }

    public String getMTM_NAME_EN() {
        return MTM_NAME_EN;
    }

    public void setMTM_NAME_EN(String MTM_NAME_EN) {
        this.MTM_NAME_EN = MTM_NAME_EN;
    }

    public String getMTM_HIPPOCRATES() {
        return MTM_HIPPOCRATES;
    }

    public void setMTM_HIPPOCRATES(String MTM_HIPPOCRATES) {
        this.MTM_HIPPOCRATES = MTM_HIPPOCRATES;
    }

    public String getMTM_NATIONALITY() {
        return MTM_NATIONALITY;
    }

    public void setMTM_NATIONALITY(String MTM_NATIONALITY) {
        this.MTM_NATIONALITY = MTM_NATIONALITY;
    }

    public Long getMTM_BIRTHDAY() {
        return MTM_BIRTHDAY;
    }

    public void setMTM_BIRTHDAY(Long MTM_BIRTHDAY) {
        this.MTM_BIRTHDAY = MTM_BIRTHDAY;
    }

    public Long getMTM_REGISTER_TIME() {
        return MTM_REGISTER_TIME;
    }

    public void setMTM_REGISTER_TIME(Long MTM_REGISTER_TIME) {
        this.MTM_REGISTER_TIME = MTM_REGISTER_TIME;
    }

    public String getPASS_ACTIVE_STATE() {
        return PASS_ACTIVE_STATE;
    }

    public void setPASS_ACTIVE_STATE(String PASS_ACTIVE_STATE) {
        this.PASS_ACTIVE_STATE = PASS_ACTIVE_STATE;
    }

    public String getREALNAME_ATTESTATION() {
        return REALNAME_ATTESTATION;
    }

    public void setREALNAME_ATTESTATION(String REALNAME_ATTESTATION) {
        this.REALNAME_ATTESTATION = REALNAME_ATTESTATION;
    }

    public String getATTESTATION_MODE() {
        return ATTESTATION_MODE;
    }

    public void setATTESTATION_MODE(String ATTESTATION_MODE) {
        this.ATTESTATION_MODE = ATTESTATION_MODE;
    }

    public Long getATTESTATION_TIME() {
        return ATTESTATION_TIME;
    }

    public void setATTESTATION_TIME(Long ATTESTATION_TIME) {
        this.ATTESTATION_TIME = ATTESTATION_TIME;
    }

    public String getMTM_MOBILE() {
        return MTM_MOBILE;
    }

    public void setMTM_MOBILE(String MTM_MOBILE) {
        this.MTM_MOBILE = MTM_MOBILE;
    }

    public String getMAIN_ID_TYPE() {
        return MAIN_ID_TYPE;
    }

    public void setMAIN_ID_TYPE(String MAIN_ID_TYPE) {
        this.MAIN_ID_TYPE = MAIN_ID_TYPE;
    }

    public String getMAIN_ID_NUM() {
        return MAIN_ID_NUM;
    }

    public void setMAIN_ID_NUM(String MAIN_ID_NUM) {
        this.MAIN_ID_NUM = MAIN_ID_NUM;
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

    public String getLINK_ID() {
        return LINK_ID;
    }

    public void setLINK_ID(String LINK_ID) {
        this.LINK_ID = LINK_ID;
    }

    public String getLINK_SOURCE() {
        return LINK_SOURCE;
    }

    public void setLINK_SOURCE(String LINK_SOURCE) {
        this.LINK_SOURCE = LINK_SOURCE;
    }

    public String getGESTURE_STATUS() {
        return GESTURE_STATUS;
    }

    public void setGESTURE_STATUS(String GESTURE_STATUS) {
        this.GESTURE_STATUS = GESTURE_STATUS;
    }

    public String getGESTURE_PASS() {
        return GESTURE_PASS;
    }

    public void setGESTURE_PASS(String GESTURE_PASS) {
        this.GESTURE_PASS = GESTURE_PASS;
    }

    public String getGESTURE_TIP() {
        return GESTURE_TIP;
    }

    public void setGESTURE_TIP(String GESTURE_TIP) {
        this.GESTURE_TIP = GESTURE_TIP;
    }

    public String getTIMING_TASK_STATUS() {
        return TIMING_TASK_STATUS;
    }

    public void setTIMING_TASK_STATUS(String TIMING_TASK_STATUS) {
        this.TIMING_TASK_STATUS = TIMING_TASK_STATUS;
    }

    public String getLIP_ATTESTATION() {
        return LIP_ATTESTATION;
    }

    public void setLIP_ATTESTATION(String LIP_ATTESTATION) {
        this.LIP_ATTESTATION = LIP_ATTESTATION;
    }

    public String getREGISTER_STATUS() {
        return REGISTER_STATUS;
    }

    public void setREGISTER_STATUS(String REGISTER_STATUS) {
        this.REGISTER_STATUS = REGISTER_STATUS;
    }

    public String getPRIVATE_POLICY_STATUS() {
        return PRIVATE_POLICY_STATUS;
    }

    public void setPRIVATE_POLICY_STATUS(String PRIVATE_POLICY_STATUS) {
        this.PRIVATE_POLICY_STATUS = PRIVATE_POLICY_STATUS;
    }

    public Long getPRIVATE_POLICY_SEND_TIME() {
        return PRIVATE_POLICY_SEND_TIME;
    }

    public void setPRIVATE_POLICY_SEND_TIME(Long PRIVATE_POLICY_SEND_TIME) {
        this.PRIVATE_POLICY_SEND_TIME = PRIVATE_POLICY_SEND_TIME;
    }

    public Long getPRIVATE_POLICY_CONFIRM_TIME() {
        return PRIVATE_POLICY_CONFIRM_TIME;
    }

    public void setPRIVATE_POLICY_CONFIRM_TIME(Long PRIVATE_POLICY_CONFIRM_TIME) {
        this.PRIVATE_POLICY_CONFIRM_TIME = PRIVATE_POLICY_CONFIRM_TIME;
    }

    public Long getLEVEL_UPDATE_TIME() {
        return LEVEL_UPDATE_TIME;
    }

    public void setLEVEL_UPDATE_TIME(Long LEVEL_UPDATE_TIME) {
        this.LEVEL_UPDATE_TIME = LEVEL_UPDATE_TIME;
    }

    public String getORI_LEVEL() {
        return ORI_LEVEL;
    }

    public void setORI_LEVEL(String ORI_LEVEL) {
        this.ORI_LEVEL = ORI_LEVEL;
    }

    public Long getUPPWD_DATE() {
        return UPPWD_DATE;
    }

    public void setUPPWD_DATE(Long UPPWD_DATE) {
        this.UPPWD_DATE = UPPWD_DATE;
    }

    public String getDIRECT_SELL_USER_ID() {
        return DIRECT_SELL_USER_ID;
    }

    public void setDIRECT_SELL_USER_ID(String DIRECT_SELL_USER_ID) {
        this.DIRECT_SELL_USER_ID = DIRECT_SELL_USER_ID;
    }
}
