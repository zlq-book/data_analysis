package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;


/**
 * @author kuangaihua
 * @date 2025/7/7 17:47
 */
public class TOdsHytdSysRegister {
    @JsonProperty("ID")
    private Long ID;
    @JsonProperty("CRM_MEMBER_ID")
    private String CRM_MEMBER_ID;
    @JsonProperty("MEMBER_NUMBER")
    private String MEMBER_NUMBER;
    @JsonProperty("CN_LAST_NAME")
    private String CN_LAST_NAME;
    @JsonProperty("CN_FIRST_NAME")
    private String CN_FIRST_NAME;
    @JsonProperty("LAST_NAME")
    private String LAST_NAME;
    @JsonProperty("FIRST_NAME")
    private String FIRST_NAME;
    @JsonProperty("GENDER")
    private String GENDER;
    @JsonProperty("BIRTHDAY")
    private String BIRTHDAY;
    @JsonProperty("CREDENTIAL_TYPE")
    private String CREDENTIAL_TYPE;
    @JsonProperty("CREDENTIAL_NUM")
    private String CREDENTIAL_NUM;
    @JsonProperty("NATIONALITY")
    private String NATIONALITY;
    @JsonProperty("LANGUAGE")
    private String LANGUAGE;
    @JsonProperty("PIN_QUESTION")
    private String PIN_QUESTION;
    @JsonProperty("PIN_ANSWER")
    private String PIN_ANSWER;
    @JsonProperty("EMAIL_TYPE")
    private String EMAIL_TYPE;
    @JsonProperty("EMAIL_ADDR")
    private String EMAIL_ADDR;
    @JsonProperty("COMPANY")
    private String COMPANY;
    @JsonProperty("DEPARTMENT")
    private String DEPARTMENT;
    @JsonProperty("CARD_STATUS")
    private String CARD_STATUS;
    @JsonProperty("PIN_STATUS")
    private String PIN_STATUS;
    @JsonProperty("ADDRESS_TYPE")
    private String ADDRESS_TYPE;
    @JsonProperty("COUNTRY")
    private String COUNTRY;
    @JsonProperty("STATE")
    private String STATE;
    @JsonProperty("CITY")
    private String CITY;
    @JsonProperty("STREET")
    private String STREET;
    @JsonProperty("POSTAL_CODE")
    private String POSTAL_CODE;
    @JsonProperty("PHONE_TYPE")
    private String PHONE_TYPE;
    @JsonProperty("COUNTRY_CODE")
    private String COUNTRY_CODE;
    @JsonProperty("PHONE_NUM")
    private String PHONE_NUM;
    @JsonProperty("PARENT_MEMBER_NUM")
    private String PARENT_MEMBER_NUM;
    @JsonProperty("FAST_CREATE_FLAG")
    private String FAST_CREATE_FLAG;
    @JsonProperty("FORCE_CREATE_FLAG")
    private String FORCE_CREATE_FLAG;
    @JsonProperty("NO_PRO_SMS_FLAG")
    private String NO_PRO_SMS_FLAG;
    @JsonProperty("NO_PRO_EMAIL_FLAG")
    private String NO_PRO_EMAIL_FLAG;
    @JsonProperty("SUBMIT_PERSON")
    private String SUBMIT_PERSON;
    @JsonProperty("CHANNEL_ID")
    private String CHANNEL_ID;
    @JsonProperty("CHANNEL_NAME")
    private String CHANNEL_NAME;
    @JsonProperty("REGISTER_STATUS")
    private String REGISTER_STATUS;
    @JsonProperty("CRM_CODE")
    private String CRM_CODE;
    @JsonProperty("CRMR_MSG")
    private String CRMR_MSG;
    @JsonProperty("CREATOR")
    private Long CREATOR;
    @JsonProperty("CREATE_DATE")
    private Long CREATE_DATE;
    @JsonProperty("REGISTER_CHANNEL")
    private String REGISTER_CHANNEL;
    @JsonProperty("ETL_DATE")
    private LocalDate ETL_DATE;

    public Long getID() {
        return ID;
    }

    @Override
    public String toString() {
        return "TOdsHytdSysRegister{" +
                "ID=" + ID +
                ", CRM_MEMBER_ID='" + CRM_MEMBER_ID + '\'' +
                ", MEMBER_NUMBER='" + MEMBER_NUMBER + '\'' +
                ", CN_LAST_NAME='" + CN_LAST_NAME + '\'' +
                ", CN_FIRST_NAME='" + CN_FIRST_NAME + '\'' +
                ", LAST_NAME='" + LAST_NAME + '\'' +
                ", FIRST_NAME='" + FIRST_NAME + '\'' +
                ", GENDER='" + GENDER + '\'' +
                ", BIRTHDAY='" + BIRTHDAY + '\'' +
                ", CREDENTIAL_TYPE='" + CREDENTIAL_TYPE + '\'' +
                ", CREDENTIAL_NUM='" + CREDENTIAL_NUM + '\'' +
                ", NATIONALITY='" + NATIONALITY + '\'' +
                ", LANGUAGE='" + LANGUAGE + '\'' +
                ", PIN_QUESTION='" + PIN_QUESTION + '\'' +
                ", PIN_ANSWER='" + PIN_ANSWER + '\'' +
                ", EMAIL_TYPE='" + EMAIL_TYPE + '\'' +
                ", EMAIL_ADDR='" + EMAIL_ADDR + '\'' +
                ", COMPANY='" + COMPANY + '\'' +
                ", DEPARTMENT='" + DEPARTMENT + '\'' +
                ", CARD_STATUS='" + CARD_STATUS + '\'' +
                ", PIN_STATUS='" + PIN_STATUS + '\'' +
                ", ADDRESS_TYPE='" + ADDRESS_TYPE + '\'' +
                ", COUNTRY='" + COUNTRY + '\'' +
                ", STATE='" + STATE + '\'' +
                ", CITY='" + CITY + '\'' +
                ", STREET='" + STREET + '\'' +
                ", POSTAL_CODE='" + POSTAL_CODE + '\'' +
                ", PHONE_TYPE='" + PHONE_TYPE + '\'' +
                ", COUNTRY_CODE='" + COUNTRY_CODE + '\'' +
                ", PHONE_NUM='" + PHONE_NUM + '\'' +
                ", PARENT_MEMBER_NUM='" + PARENT_MEMBER_NUM + '\'' +
                ", FAST_CREATE_FLAG='" + FAST_CREATE_FLAG + '\'' +
                ", FORCE_CREATE_FLAG='" + FORCE_CREATE_FLAG + '\'' +
                ", NO_PRO_SMS_FLAG='" + NO_PRO_SMS_FLAG + '\'' +
                ", NO_PRO_EMAIL_FLAG='" + NO_PRO_EMAIL_FLAG + '\'' +
                ", SUBMIT_PERSON='" + SUBMIT_PERSON + '\'' +
                ", CHANNEL_ID='" + CHANNEL_ID + '\'' +
                ", CHANNEL_NAME='" + CHANNEL_NAME + '\'' +
                ", REGISTER_STATUS='" + REGISTER_STATUS + '\'' +
                ", CRM_CODE='" + CRM_CODE + '\'' +
                ", CRMR_MSG='" + CRMR_MSG + '\'' +
                ", CREATOR=" + CREATOR +
                ", CREATE_DATE=" + CREATE_DATE +
                ", REGISTER_CHANNEL='" + REGISTER_CHANNEL + '\'' +
                ", ETL_DATE=" + ETL_DATE +
                '}';
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public String getCRM_MEMBER_ID() {
        return CRM_MEMBER_ID;
    }

    public void setCRM_MEMBER_ID(String CRM_MEMBER_ID) {
        this.CRM_MEMBER_ID = CRM_MEMBER_ID;
    }

    public String getMEMBER_NUMBER() {
        return MEMBER_NUMBER;
    }

    public void setMEMBER_NUMBER(String MEMBER_NUMBER) {
        this.MEMBER_NUMBER = MEMBER_NUMBER;
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

    public String getGENDER() {
        return GENDER;
    }

    public void setGENDER(String GENDER) {
        this.GENDER = GENDER;
    }

    public String getBIRTHDAY() {
        return BIRTHDAY;
    }

    public void setBIRTHDAY(String BIRTHDAY) {
        this.BIRTHDAY = BIRTHDAY;
    }

    public String getCREDENTIAL_TYPE() {
        return CREDENTIAL_TYPE;
    }

    public void setCREDENTIAL_TYPE(String CREDENTIAL_TYPE) {
        this.CREDENTIAL_TYPE = CREDENTIAL_TYPE;
    }

    public String getCREDENTIAL_NUM() {
        return CREDENTIAL_NUM;
    }

    public void setCREDENTIAL_NUM(String CREDENTIAL_NUM) {
        this.CREDENTIAL_NUM = CREDENTIAL_NUM;
    }

    public String getNATIONALITY() {
        return NATIONALITY;
    }

    public void setNATIONALITY(String NATIONALITY) {
        this.NATIONALITY = NATIONALITY;
    }

    public String getLANGUAGE() {
        return LANGUAGE;
    }

    public void setLANGUAGE(String LANGUAGE) {
        this.LANGUAGE = LANGUAGE;
    }

    public String getPIN_QUESTION() {
        return PIN_QUESTION;
    }

    public void setPIN_QUESTION(String PIN_QUESTION) {
        this.PIN_QUESTION = PIN_QUESTION;
    }

    public String getPIN_ANSWER() {
        return PIN_ANSWER;
    }

    public void setPIN_ANSWER(String PIN_ANSWER) {
        this.PIN_ANSWER = PIN_ANSWER;
    }

    public String getEMAIL_TYPE() {
        return EMAIL_TYPE;
    }

    public void setEMAIL_TYPE(String EMAIL_TYPE) {
        this.EMAIL_TYPE = EMAIL_TYPE;
    }

    public String getEMAIL_ADDR() {
        return EMAIL_ADDR;
    }

    public void setEMAIL_ADDR(String EMAIL_ADDR) {
        this.EMAIL_ADDR = EMAIL_ADDR;
    }

    public String getCOMPANY() {
        return COMPANY;
    }

    public void setCOMPANY(String COMPANY) {
        this.COMPANY = COMPANY;
    }

    public String getDEPARTMENT() {
        return DEPARTMENT;
    }

    public void setDEPARTMENT(String DEPARTMENT) {
        this.DEPARTMENT = DEPARTMENT;
    }

    public String getCARD_STATUS() {
        return CARD_STATUS;
    }

    public void setCARD_STATUS(String CARD_STATUS) {
        this.CARD_STATUS = CARD_STATUS;
    }

    public String getPIN_STATUS() {
        return PIN_STATUS;
    }

    public void setPIN_STATUS(String PIN_STATUS) {
        this.PIN_STATUS = PIN_STATUS;
    }

    public String getADDRESS_TYPE() {
        return ADDRESS_TYPE;
    }

    public void setADDRESS_TYPE(String ADDRESS_TYPE) {
        this.ADDRESS_TYPE = ADDRESS_TYPE;
    }

    public String getCOUNTRY() {
        return COUNTRY;
    }

    public void setCOUNTRY(String COUNTRY) {
        this.COUNTRY = COUNTRY;
    }

    public String getSTATE() {
        return STATE;
    }

    public void setSTATE(String STATE) {
        this.STATE = STATE;
    }

    public String getCITY() {
        return CITY;
    }

    public void setCITY(String CITY) {
        this.CITY = CITY;
    }

    public String getSTREET() {
        return STREET;
    }

    public void setSTREET(String STREET) {
        this.STREET = STREET;
    }

    public String getPOSTAL_CODE() {
        return POSTAL_CODE;
    }

    public void setPOSTAL_CODE(String POSTAL_CODE) {
        this.POSTAL_CODE = POSTAL_CODE;
    }

    public String getPHONE_TYPE() {
        return PHONE_TYPE;
    }

    public void setPHONE_TYPE(String PHONE_TYPE) {
        this.PHONE_TYPE = PHONE_TYPE;
    }

    public String getCOUNTRY_CODE() {
        return COUNTRY_CODE;
    }

    public void setCOUNTRY_CODE(String COUNTRY_CODE) {
        this.COUNTRY_CODE = COUNTRY_CODE;
    }

    public String getPHONE_NUM() {
        return PHONE_NUM;
    }

    public void setPHONE_NUM(String PHONE_NUM) {
        this.PHONE_NUM = PHONE_NUM;
    }

    public String getPARENT_MEMBER_NUM() {
        return PARENT_MEMBER_NUM;
    }

    public void setPARENT_MEMBER_NUM(String PARENT_MEMBER_NUM) {
        this.PARENT_MEMBER_NUM = PARENT_MEMBER_NUM;
    }

    public String getFAST_CREATE_FLAG() {
        return FAST_CREATE_FLAG;
    }

    public void setFAST_CREATE_FLAG(String FAST_CREATE_FLAG) {
        this.FAST_CREATE_FLAG = FAST_CREATE_FLAG;
    }

    public String getFORCE_CREATE_FLAG() {
        return FORCE_CREATE_FLAG;
    }

    public void setFORCE_CREATE_FLAG(String FORCE_CREATE_FLAG) {
        this.FORCE_CREATE_FLAG = FORCE_CREATE_FLAG;
    }

    public String getNO_PRO_SMS_FLAG() {
        return NO_PRO_SMS_FLAG;
    }

    public void setNO_PRO_SMS_FLAG(String NO_PRO_SMS_FLAG) {
        this.NO_PRO_SMS_FLAG = NO_PRO_SMS_FLAG;
    }

    public String getNO_PRO_EMAIL_FLAG() {
        return NO_PRO_EMAIL_FLAG;
    }

    public void setNO_PRO_EMAIL_FLAG(String NO_PRO_EMAIL_FLAG) {
        this.NO_PRO_EMAIL_FLAG = NO_PRO_EMAIL_FLAG;
    }

    public String getSUBMIT_PERSON() {
        return SUBMIT_PERSON;
    }

    public void setSUBMIT_PERSON(String SUBMIT_PERSON) {
        this.SUBMIT_PERSON = SUBMIT_PERSON;
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

    public String getREGISTER_STATUS() {
        return REGISTER_STATUS;
    }

    public void setREGISTER_STATUS(String REGISTER_STATUS) {
        this.REGISTER_STATUS = REGISTER_STATUS;
    }

    public String getCRM_CODE() {
        return CRM_CODE;
    }

    public void setCRM_CODE(String CRM_CODE) {
        this.CRM_CODE = CRM_CODE;
    }

    public String getCRMR_MSG() {
        return CRMR_MSG;
    }

    public void setCRMR_MSG(String CRMR_MSG) {
        this.CRMR_MSG = CRMR_MSG;
    }

    public Long getCREATOR() {
        return CREATOR;
    }

    public void setCREATOR(Long CREATOR) {
        this.CREATOR = CREATOR;
    }

    public Long getCREATE_DATE() {
        return CREATE_DATE;
    }

    public void setCREATE_DATE(Long CREATE_DATE) {
        this.CREATE_DATE = CREATE_DATE;
    }

    public String getREGISTER_CHANNEL() {
        return REGISTER_CHANNEL;
    }

    public void setREGISTER_CHANNEL(String REGISTER_CHANNEL) {
        this.REGISTER_CHANNEL = REGISTER_CHANNEL;
    }

    public LocalDate getETL_DATE() {
        return ETL_DATE;
    }

    public void setETL_DATE(LocalDate ETL_DATE) {
        this.ETL_DATE = ETL_DATE;
    }

    public TOdsHytdSysRegister() {
    }
}
