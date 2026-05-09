package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;


/**
 * @author kuangaihua
 * @date 2025/7/7 17:47
 */
public class TOdsHytdSysDictData {
    @JsonProperty("ID")
    private Long ID;

    @JsonProperty("DICT_TYPE_ID")
    private Long DICT_TYPE_ID;

    @JsonProperty("DICT_LABEL")
    private String DICT_LABEL;

    @JsonProperty("DICT_VALUE")
    private String DICT_VALUE;

    @JsonProperty("REMARK")
    private String REMARK;

    @JsonProperty("SORT")
    private Long SORT;

    @JsonProperty("CREATOR")
    private Long CREATOR;

    @JsonProperty("CREATE_DATE")
    private Long CREATE_DATE;

    @JsonProperty("UPDATER")
    private Long UPDATER;

    @JsonProperty("UPDATE_DATE")
    private Long UPDATE_DATE;

    public TOdsHytdSysDictData() {
    }

    public Long getCREATE_DATE() {
        return CREATE_DATE;
    }

    public void setCREATE_DATE(Long CREATE_DATE) {
        this.CREATE_DATE = CREATE_DATE;
    }

    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public Long getDICT_TYPE_ID() {
        return DICT_TYPE_ID;
    }

    public void setDICT_TYPE_ID(Long DICT_TYPE_ID) {
        this.DICT_TYPE_ID = DICT_TYPE_ID;
    }

    public String getDICT_LABEL() {
        return DICT_LABEL;
    }

    public void setDICT_LABEL(String DICT_LABEL) {
        this.DICT_LABEL = DICT_LABEL;
    }

    public String getDICT_VALUE() {
        return DICT_VALUE;
    }

    public void setDICT_VALUE(String DICT_VALUE) {
        this.DICT_VALUE = DICT_VALUE;
    }

    public String getREMARK() {
        return REMARK;
    }

    public void setREMARK(String REMARK) {
        this.REMARK = REMARK;
    }

    public Long getSORT() {
        return SORT;
    }

    public void setSORT(Long SORT) {
        this.SORT = SORT;
    }

    public Long getCREATOR() {
        return CREATOR;
    }

    public void setCREATOR(Long CREATOR) {
        this.CREATOR = CREATOR;
    }

    public Long getUPDATER() {
        return UPDATER;
    }

    public void setUPDATER(Long UPDATER) {
        this.UPDATER = UPDATER;
    }

    public Long getUPDATE_DATE() {
        return UPDATE_DATE;
    }

    public void setUPDATE_DATE(Long UPDATE_DATE) {
        this.UPDATE_DATE = UPDATE_DATE;
    }
}
