package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TOdsLyxOrderChangeRefundInfo {
    @JsonProperty("ID")
    private Long ID;
    @JsonProperty("ORDER_NO")
    private String ORDER_NO;
    @JsonProperty("TOTAL_REFUND_STATUS")
    private String TOTAL_REFUND_STATUS;
    @JsonProperty("ORDER_TYPE")
    private String ORDER_TYPE;
    @JsonProperty("OLD_ORDER_NO")
    private String OLD_ORDER_NO;
    @JsonProperty("ORIGINAL_ORDER_NO")
    private String ORIGINAL_ORDER_NO;
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
    @JsonProperty("CHANGE_FEE_TOTAL")
    private String CHANGE_FEE_TOTAL;
    @JsonProperty("COMPENSATION_FEE_TOTAL")
    private String COMPENSATION_FEE_TOTAL;
    @JsonProperty("LY_SUM")
    private String LY_SUM;
    @JsonProperty("CASH_SUM")
    private String CASH_SUM;
    @JsonProperty("CHANGE_PERPSON")
    private String CHANGE_PERPSON;
    @JsonProperty("CHANGE_TIME")
    private Long CHANGE_TIME;
    @JsonProperty("REFUND_PERPSON")
    private String REFUND_PERPSON;
    @JsonProperty("REFUND_TIME")
    private Long REFUND_TIME;
    @JsonProperty("REFUND_LY_VALUE")
    private String REFUND_LY_VALUE;
    @JsonProperty("REFUND_LY_STATUS")
    private String REFUND_LY_STATUS;
    @JsonProperty("LY_EXPIRES_TIME")
    private Long LY_EXPIRES_TIME;
    @JsonProperty("REFUND_CASH")
    private String REFUND_CASH;
    @JsonProperty("REFUND_CASH_STATUS")
    private String REFUND_CASH_STATUS;
    @JsonProperty("AIRPORT_TAX_TOTAL")
    private String AIRPORT_TAX_TOTAL;
    @JsonProperty("FUEL_TAX_TOTAL")
    private String FUEL_TAX_TOTAL;
    @JsonProperty("INSURE_FEE_TOTAL")
    private String INSURE_FEE_TOTAL;
    @JsonProperty("CHANGE_SUM_FEE")
    private String CHANGE_SUM_FEE;
    @JsonProperty("LAST_CHANGE_FEE")
    private String LAST_CHANGE_FEE;
    @JsonProperty("REFUND_TYPE")
    private String REFUND_TYPE;
    @JsonProperty("NOVOLUNTEER_REASON")
    private String NOVOLUNTEER_REASON;
    @JsonProperty("REFUND_EXPLAIN")
    private String REFUND_EXPLAIN;
    @JsonProperty("REFUND_EVIDENCE")
    private String REFUND_EVIDENCE;
    @JsonProperty("REFUND_EVIDENCE_IMAGE")
    private String REFUND_EVIDENCE_IMAGE;
    @JsonProperty("APPLY_PERSON")
    private String APPLY_PERSON;
    @JsonProperty("APPLY_TIME")
    private Long APPLY_TIME;
    @JsonProperty("AUDIT_PERSON")
    private String AUDIT_PERSON;
    @JsonProperty("AUDIT_TIME")
    private Long AUDIT_TIME;
    @JsonProperty("PAY_ID")
    private Long PAY_ID;
    @JsonProperty("USED_LEFT_FLAG")
    private String USED_LEFT_FLAG;
    @JsonProperty("IS_ABNORMAL")
    private String IS_ABNORMAL;
    @JsonProperty("ERROR_STATUS")
    private String ERROR_STATUS;
    @JsonProperty("AUDIT_STATUS")
    private String AUDIT_STATUS;
    @JsonProperty("COMPLETE_STATUS")
    private String COMPLETE_STATUS;
    @JsonProperty("AUDIT_REMARK")
    private String AUDIT_REMARK;
    @JsonProperty("AUDIT_RESULT")
    private String AUDIT_RESULT;
    @JsonProperty("AUDIT_SECOND_PERSON")
    private String AUDIT_SECOND_PERSON;
    @JsonProperty("AUDIT_SECOND_TIME")
    private Long AUDIT_SECOND_TIME;
    @JsonProperty("AUDIT_SECOND_RESULT")
    private String AUDIT_SECOND_RESULT;
    @JsonProperty("AUDIT_SECOND_REMARK")
    private String AUDIT_SECOND_REMARK;
    @JsonProperty("CHANGE_TYPE")
    private String CHANGE_TYPE;
    @JsonProperty("CHANGE_EXPLAIN")
    private String CHANGE_EXPLAIN;
    @JsonProperty("CHANGE_NOVOLUNTEER_REASON")
    private String CHANGE_NOVOLUNTEER_REASON;
    @JsonProperty("CHANGE_EVIDENCE")
    private String CHANGE_EVIDENCE;
    @JsonProperty("CHANGE_EVIDENCE_IMAGE")
    private String CHANGE_EVIDENCE_IMAGE;
    @JsonProperty("REFUND_LY_VALUE_METHOD")
    private String REFUND_LY_VALUE_METHOD;
    @JsonProperty("EFFECTIVE_DATE")
    private Long EFFECTIVE_DATE;
    @JsonProperty("ERROR_SOLUTION")
    private String ERROR_SOLUTION;

    @Override
    public String toString() {
        return "TOdsLyxOrderChangeRefundInfo{" +
                "ID=" + ID +
                ", ORDER_NO='" + ORDER_NO + '\'' +
                ", TOTAL_REFUND_STATUS='" + TOTAL_REFUND_STATUS + '\'' +
                ", ORDER_TYPE='" + ORDER_TYPE + '\'' +
                ", OLD_ORDER_NO='" + OLD_ORDER_NO + '\'' +
                ", ORIGINAL_ORDER_NO='" + ORIGINAL_ORDER_NO + '\'' +
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
                ", CHANGE_FEE_TOTAL='" + CHANGE_FEE_TOTAL + '\'' +
                ", COMPENSATION_FEE_TOTAL='" + COMPENSATION_FEE_TOTAL + '\'' +
                ", LY_SUM='" + LY_SUM + '\'' +
                ", CASH_SUM='" + CASH_SUM + '\'' +
                ", CHANGE_PERPSON='" + CHANGE_PERPSON + '\'' +
                ", CHANGE_TIME=" + CHANGE_TIME +
                ", REFUND_PERPSON='" + REFUND_PERPSON + '\'' +
                ", REFUND_TIME=" + REFUND_TIME +
                ", REFUND_LY_VALUE='" + REFUND_LY_VALUE + '\'' +
                ", REFUND_LY_STATUS='" + REFUND_LY_STATUS + '\'' +
                ", LY_EXPIRES_TIME=" + LY_EXPIRES_TIME +
                ", REFUND_CASH='" + REFUND_CASH + '\'' +
                ", REFUND_CASH_STATUS='" + REFUND_CASH_STATUS + '\'' +
                ", AIRPORT_TAX_TOTAL='" + AIRPORT_TAX_TOTAL + '\'' +
                ", FUEL_TAX_TOTAL='" + FUEL_TAX_TOTAL + '\'' +
                ", INSURE_FEE_TOTAL='" + INSURE_FEE_TOTAL + '\'' +
                ", CHANGE_SUM_FEE='" + CHANGE_SUM_FEE + '\'' +
                ", LAST_CHANGE_FEE='" + LAST_CHANGE_FEE + '\'' +
                ", REFUND_TYPE='" + REFUND_TYPE + '\'' +
                ", NOVOLUNTEER_REASON='" + NOVOLUNTEER_REASON + '\'' +
                ", REFUND_EXPLAIN='" + REFUND_EXPLAIN + '\'' +
                ", REFUND_EVIDENCE='" + REFUND_EVIDENCE + '\'' +
                ", REFUND_EVIDENCE_IMAGE='" + REFUND_EVIDENCE_IMAGE + '\'' +
                ", APPLY_PERSON='" + APPLY_PERSON + '\'' +
                ", APPLY_TIME=" + APPLY_TIME +
                ", AUDIT_PERSON='" + AUDIT_PERSON + '\'' +
                ", AUDIT_TIME=" + AUDIT_TIME +
                ", PAY_ID=" + PAY_ID +
                ", USED_LEFT_FLAG='" + USED_LEFT_FLAG + '\'' +
                ", IS_ABNORMAL='" + IS_ABNORMAL + '\'' +
                ", ERROR_STATUS='" + ERROR_STATUS + '\'' +
                ", AUDIT_STATUS='" + AUDIT_STATUS + '\'' +
                ", COMPLETE_STATUS='" + COMPLETE_STATUS + '\'' +
                ", AUDIT_REMARK='" + AUDIT_REMARK + '\'' +
                ", AUDIT_RESULT='" + AUDIT_RESULT + '\'' +
                ", AUDIT_SECOND_PERSON='" + AUDIT_SECOND_PERSON + '\'' +
                ", AUDIT_SECOND_TIME=" + AUDIT_SECOND_TIME +
                ", AUDIT_SECOND_RESULT='" + AUDIT_SECOND_RESULT + '\'' +
                ", AUDIT_SECOND_REMARK='" + AUDIT_SECOND_REMARK + '\'' +
                ", CHANGE_TYPE='" + CHANGE_TYPE + '\'' +
                ", CHANGE_EXPLAIN='" + CHANGE_EXPLAIN + '\'' +
                ", CHANGE_NOVOLUNTEER_REASON='" + CHANGE_NOVOLUNTEER_REASON + '\'' +
                ", CHANGE_EVIDENCE='" + CHANGE_EVIDENCE + '\'' +
                ", CHANGE_EVIDENCE_IMAGE='" + CHANGE_EVIDENCE_IMAGE + '\'' +
                ", REFUND_LY_VALUE_METHOD='" + REFUND_LY_VALUE_METHOD + '\'' +
                ", EFFECTIVE_DATE=" + EFFECTIVE_DATE +
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

    public String getTOTAL_REFUND_STATUS() {
        return TOTAL_REFUND_STATUS;
    }

    public void setTOTAL_REFUND_STATUS(String TOTAL_REFUND_STATUS) {
        this.TOTAL_REFUND_STATUS = TOTAL_REFUND_STATUS;
    }

    public String getORDER_TYPE() {
        return ORDER_TYPE;
    }

    public void setORDER_TYPE(String ORDER_TYPE) {
        this.ORDER_TYPE = ORDER_TYPE;
    }

    public String getOLD_ORDER_NO() {
        return OLD_ORDER_NO;
    }

    public void setOLD_ORDER_NO(String OLD_ORDER_NO) {
        this.OLD_ORDER_NO = OLD_ORDER_NO;
    }

    public String getORIGINAL_ORDER_NO() {
        return ORIGINAL_ORDER_NO;
    }

    public void setORIGINAL_ORDER_NO(String ORIGINAL_ORDER_NO) {
        this.ORIGINAL_ORDER_NO = ORIGINAL_ORDER_NO;
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

    public String getCHANGE_FEE_TOTAL() {
        return CHANGE_FEE_TOTAL;
    }

    public void setCHANGE_FEE_TOTAL(String CHANGE_FEE_TOTAL) {
        this.CHANGE_FEE_TOTAL = CHANGE_FEE_TOTAL;
    }

    public String getCOMPENSATION_FEE_TOTAL() {
        return COMPENSATION_FEE_TOTAL;
    }

    public void setCOMPENSATION_FEE_TOTAL(String COMPENSATION_FEE_TOTAL) {
        this.COMPENSATION_FEE_TOTAL = COMPENSATION_FEE_TOTAL;
    }

    public String getLY_SUM() {
        return LY_SUM;
    }

    public void setLY_SUM(String LY_SUM) {
        this.LY_SUM = LY_SUM;
    }

    public String getCASH_SUM() {
        return CASH_SUM;
    }

    public void setCASH_SUM(String CASH_SUM) {
        this.CASH_SUM = CASH_SUM;
    }

    public String getCHANGE_PERPSON() {
        return CHANGE_PERPSON;
    }

    public void setCHANGE_PERPSON(String CHANGE_PERPSON) {
        this.CHANGE_PERPSON = CHANGE_PERPSON;
    }

    public Long getCHANGE_TIME() {
        return CHANGE_TIME;
    }

    public void setCHANGE_TIME(Long CHANGE_TIME) {
        this.CHANGE_TIME = CHANGE_TIME;
    }

    public String getREFUND_PERPSON() {
        return REFUND_PERPSON;
    }

    public void setREFUND_PERPSON(String REFUND_PERPSON) {
        this.REFUND_PERPSON = REFUND_PERPSON;
    }

    public Long getREFUND_TIME() {
        return REFUND_TIME;
    }

    public void setREFUND_TIME(Long REFUND_TIME) {
        this.REFUND_TIME = REFUND_TIME;
    }

    public String getREFUND_LY_VALUE() {
        return REFUND_LY_VALUE;
    }

    public void setREFUND_LY_VALUE(String REFUND_LY_VALUE) {
        this.REFUND_LY_VALUE = REFUND_LY_VALUE;
    }

    public String getREFUND_LY_STATUS() {
        return REFUND_LY_STATUS;
    }

    public void setREFUND_LY_STATUS(String REFUND_LY_STATUS) {
        this.REFUND_LY_STATUS = REFUND_LY_STATUS;
    }

    public Long getLY_EXPIRES_TIME() {
        return LY_EXPIRES_TIME;
    }

    public void setLY_EXPIRES_TIME(Long LY_EXPIRES_TIME) {
        this.LY_EXPIRES_TIME = LY_EXPIRES_TIME;
    }

    public String getREFUND_CASH() {
        return REFUND_CASH;
    }

    public void setREFUND_CASH(String REFUND_CASH) {
        this.REFUND_CASH = REFUND_CASH;
    }

    public String getREFUND_CASH_STATUS() {
        return REFUND_CASH_STATUS;
    }

    public void setREFUND_CASH_STATUS(String REFUND_CASH_STATUS) {
        this.REFUND_CASH_STATUS = REFUND_CASH_STATUS;
    }

    public String getAIRPORT_TAX_TOTAL() {
        return AIRPORT_TAX_TOTAL;
    }

    public void setAIRPORT_TAX_TOTAL(String AIRPORT_TAX_TOTAL) {
        this.AIRPORT_TAX_TOTAL = AIRPORT_TAX_TOTAL;
    }

    public String getFUEL_TAX_TOTAL() {
        return FUEL_TAX_TOTAL;
    }

    public void setFUEL_TAX_TOTAL(String FUEL_TAX_TOTAL) {
        this.FUEL_TAX_TOTAL = FUEL_TAX_TOTAL;
    }

    public String getINSURE_FEE_TOTAL() {
        return INSURE_FEE_TOTAL;
    }

    public void setINSURE_FEE_TOTAL(String INSURE_FEE_TOTAL) {
        this.INSURE_FEE_TOTAL = INSURE_FEE_TOTAL;
    }

    public String getCHANGE_SUM_FEE() {
        return CHANGE_SUM_FEE;
    }

    public void setCHANGE_SUM_FEE(String CHANGE_SUM_FEE) {
        this.CHANGE_SUM_FEE = CHANGE_SUM_FEE;
    }

    public String getLAST_CHANGE_FEE() {
        return LAST_CHANGE_FEE;
    }

    public void setLAST_CHANGE_FEE(String LAST_CHANGE_FEE) {
        this.LAST_CHANGE_FEE = LAST_CHANGE_FEE;
    }

    public String getREFUND_TYPE() {
        return REFUND_TYPE;
    }

    public void setREFUND_TYPE(String REFUND_TYPE) {
        this.REFUND_TYPE = REFUND_TYPE;
    }

    public String getNOVOLUNTEER_REASON() {
        return NOVOLUNTEER_REASON;
    }

    public void setNOVOLUNTEER_REASON(String NOVOLUNTEER_REASON) {
        this.NOVOLUNTEER_REASON = NOVOLUNTEER_REASON;
    }

    public String getREFUND_EXPLAIN() {
        return REFUND_EXPLAIN;
    }

    public void setREFUND_EXPLAIN(String REFUND_EXPLAIN) {
        this.REFUND_EXPLAIN = REFUND_EXPLAIN;
    }

    public String getREFUND_EVIDENCE() {
        return REFUND_EVIDENCE;
    }

    public void setREFUND_EVIDENCE(String REFUND_EVIDENCE) {
        this.REFUND_EVIDENCE = REFUND_EVIDENCE;
    }

    public String getREFUND_EVIDENCE_IMAGE() {
        return REFUND_EVIDENCE_IMAGE;
    }

    public void setREFUND_EVIDENCE_IMAGE(String REFUND_EVIDENCE_IMAGE) {
        this.REFUND_EVIDENCE_IMAGE = REFUND_EVIDENCE_IMAGE;
    }

    public String getAPPLY_PERSON() {
        return APPLY_PERSON;
    }

    public void setAPPLY_PERSON(String APPLY_PERSON) {
        this.APPLY_PERSON = APPLY_PERSON;
    }

    public Long getAPPLY_TIME() {
        return APPLY_TIME;
    }

    public void setAPPLY_TIME(Long APPLY_TIME) {
        this.APPLY_TIME = APPLY_TIME;
    }

    public String getAUDIT_PERSON() {
        return AUDIT_PERSON;
    }

    public void setAUDIT_PERSON(String AUDIT_PERSON) {
        this.AUDIT_PERSON = AUDIT_PERSON;
    }

    public Long getAUDIT_TIME() {
        return AUDIT_TIME;
    }

    public void setAUDIT_TIME(Long AUDIT_TIME) {
        this.AUDIT_TIME = AUDIT_TIME;
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

    public String getIS_ABNORMAL() {
        return IS_ABNORMAL;
    }

    public void setIS_ABNORMAL(String IS_ABNORMAL) {
        this.IS_ABNORMAL = IS_ABNORMAL;
    }

    public String getERROR_STATUS() {
        return ERROR_STATUS;
    }

    public void setERROR_STATUS(String ERROR_STATUS) {
        this.ERROR_STATUS = ERROR_STATUS;
    }

    public String getAUDIT_STATUS() {
        return AUDIT_STATUS;
    }

    public void setAUDIT_STATUS(String AUDIT_STATUS) {
        this.AUDIT_STATUS = AUDIT_STATUS;
    }

    public String getCOMPLETE_STATUS() {
        return COMPLETE_STATUS;
    }

    public void setCOMPLETE_STATUS(String COMPLETE_STATUS) {
        this.COMPLETE_STATUS = COMPLETE_STATUS;
    }

    public String getAUDIT_REMARK() {
        return AUDIT_REMARK;
    }

    public void setAUDIT_REMARK(String AUDIT_REMARK) {
        this.AUDIT_REMARK = AUDIT_REMARK;
    }

    public String getAUDIT_RESULT() {
        return AUDIT_RESULT;
    }

    public void setAUDIT_RESULT(String AUDIT_RESULT) {
        this.AUDIT_RESULT = AUDIT_RESULT;
    }

    public String getAUDIT_SECOND_PERSON() {
        return AUDIT_SECOND_PERSON;
    }

    public void setAUDIT_SECOND_PERSON(String AUDIT_SECOND_PERSON) {
        this.AUDIT_SECOND_PERSON = AUDIT_SECOND_PERSON;
    }

    public Long getAUDIT_SECOND_TIME() {
        return AUDIT_SECOND_TIME;
    }

    public void setAUDIT_SECOND_TIME(Long AUDIT_SECOND_TIME) {
        this.AUDIT_SECOND_TIME = AUDIT_SECOND_TIME;
    }

    public String getAUDIT_SECOND_RESULT() {
        return AUDIT_SECOND_RESULT;
    }

    public void setAUDIT_SECOND_RESULT(String AUDIT_SECOND_RESULT) {
        this.AUDIT_SECOND_RESULT = AUDIT_SECOND_RESULT;
    }

    public String getAUDIT_SECOND_REMARK() {
        return AUDIT_SECOND_REMARK;
    }

    public void setAUDIT_SECOND_REMARK(String AUDIT_SECOND_REMARK) {
        this.AUDIT_SECOND_REMARK = AUDIT_SECOND_REMARK;
    }

    public String getCHANGE_TYPE() {
        return CHANGE_TYPE;
    }

    public void setCHANGE_TYPE(String CHANGE_TYPE) {
        this.CHANGE_TYPE = CHANGE_TYPE;
    }

    public String getCHANGE_EXPLAIN() {
        return CHANGE_EXPLAIN;
    }

    public void setCHANGE_EXPLAIN(String CHANGE_EXPLAIN) {
        this.CHANGE_EXPLAIN = CHANGE_EXPLAIN;
    }

    public String getCHANGE_NOVOLUNTEER_REASON() {
        return CHANGE_NOVOLUNTEER_REASON;
    }

    public void setCHANGE_NOVOLUNTEER_REASON(String CHANGE_NOVOLUNTEER_REASON) {
        this.CHANGE_NOVOLUNTEER_REASON = CHANGE_NOVOLUNTEER_REASON;
    }

    public String getCHANGE_EVIDENCE() {
        return CHANGE_EVIDENCE;
    }

    public void setCHANGE_EVIDENCE(String CHANGE_EVIDENCE) {
        this.CHANGE_EVIDENCE = CHANGE_EVIDENCE;
    }

    public String getCHANGE_EVIDENCE_IMAGE() {
        return CHANGE_EVIDENCE_IMAGE;
    }

    public void setCHANGE_EVIDENCE_IMAGE(String CHANGE_EVIDENCE_IMAGE) {
        this.CHANGE_EVIDENCE_IMAGE = CHANGE_EVIDENCE_IMAGE;
    }

    public String getREFUND_LY_VALUE_METHOD() {
        return REFUND_LY_VALUE_METHOD;
    }

    public void setREFUND_LY_VALUE_METHOD(String REFUND_LY_VALUE_METHOD) {
        this.REFUND_LY_VALUE_METHOD = REFUND_LY_VALUE_METHOD;
    }

    public Long getEFFECTIVE_DATE() {
        return EFFECTIVE_DATE;
    }

    public void setEFFECTIVE_DATE(Long EFFECTIVE_DATE) {
        this.EFFECTIVE_DATE = EFFECTIVE_DATE;
    }

    public String getERROR_SOLUTION() {
        return ERROR_SOLUTION;
    }

    public void setERROR_SOLUTION(String ERROR_SOLUTION) {
        this.ERROR_SOLUTION = ERROR_SOLUTION;
    }
}
