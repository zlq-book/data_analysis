package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TOdsScTbTicketPriceBase {
    @JsonProperty("AIR_CODE")
    private String AIR_CODE;
    @JsonProperty("EX_DATE")
    private String EX_DATE;
    @JsonProperty("UP_LOCATION")
    private String UP_LOCATION;
    @JsonProperty("DIS_LOCATION")
    private String DIS_LOCATION;
    @JsonProperty("PRICE_ONEWAY")
    private Double PRICE_ONEWAY;
    @JsonProperty("PRICE_TOWWAY")
    private Double PRICE_TOWWAY;
    @JsonProperty("START_DATE")
    private String START_DATE;
    @JsonProperty("END_DATE")
    private String END_DATE;
    @JsonProperty("SEAT_TYPE")
    private String SEAT_TYPE;
    @JsonProperty("OUT_LINE")
    private String OUT_LINE;
    @JsonProperty("CMD")
    private String CMD;
    @JsonProperty("DIST")
    private Long DIST;

    @Override
    public String toString() {
        return "TOdsScTbTicketPriceBase{" +
                "AIR_CODE='" + AIR_CODE + '\'' +
                ", EX_DATE='" + EX_DATE + '\'' +
                ", UP_LOCATION='" + UP_LOCATION + '\'' +
                ", DIS_LOCATION='" + DIS_LOCATION + '\'' +
                ", PRICE_ONEWAY=" + PRICE_ONEWAY +
                ", PRICE_TOWWAY=" + PRICE_TOWWAY +
                ", START_DATE='" + START_DATE + '\'' +
                ", END_DATE='" + END_DATE + '\'' +
                ", SEAT_TYPE='" + SEAT_TYPE + '\'' +
                ", OUT_LINE='" + OUT_LINE + '\'' +
                ", CMD='" + CMD + '\'' +
                ", DIST=" + DIST +
                '}';
    }

    public String getAIR_CODE() {
        return AIR_CODE;
    }

    public void setAIR_CODE(String AIR_CODE) {
        this.AIR_CODE = AIR_CODE;
    }

    public String getEX_DATE() {
        return EX_DATE;
    }

    public void setEX_DATE(String EX_DATE) {
        this.EX_DATE = EX_DATE;
    }

    public String getUP_LOCATION() {
        return UP_LOCATION;
    }

    public void setUP_LOCATION(String UP_LOCATION) {
        this.UP_LOCATION = UP_LOCATION;
    }

    public String getDIS_LOCATION() {
        return DIS_LOCATION;
    }

    public void setDIS_LOCATION(String DIS_LOCATION) {
        this.DIS_LOCATION = DIS_LOCATION;
    }

    public Double getPRICE_ONEWAY() {
        return PRICE_ONEWAY;
    }

    public void setPRICE_ONEWAY(Double PRICE_ONEWAY) {
        this.PRICE_ONEWAY = PRICE_ONEWAY;
    }

    public Double getPRICE_TOWWAY() {
        return PRICE_TOWWAY;
    }

    public void setPRICE_TOWWAY(Double PRICE_TOWWAY) {
        this.PRICE_TOWWAY = PRICE_TOWWAY;
    }

    public String getSTART_DATE() {
        return START_DATE;
    }

    public void setSTART_DATE(String START_DATE) {
        this.START_DATE = START_DATE;
    }

    public String getEND_DATE() {
        return END_DATE;
    }

    public void setEND_DATE(String END_DATE) {
        this.END_DATE = END_DATE;
    }

    public String getSEAT_TYPE() {
        return SEAT_TYPE;
    }

    public void setSEAT_TYPE(String SEAT_TYPE) {
        this.SEAT_TYPE = SEAT_TYPE;
    }

    public String getOUT_LINE() {
        return OUT_LINE;
    }

    public void setOUT_LINE(String OUT_LINE) {
        this.OUT_LINE = OUT_LINE;
    }

    public String getCMD() {
        return CMD;
    }

    public void setCMD(String CMD) {
        this.CMD = CMD;
    }

    public Long getDIST() {
        return DIST;
    }

    public void setDIST(Long DIST) {
        this.DIST = DIST;
    }
}
