package com.travelsky.dataplatform.module.ods;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Date;

/**
 * 对应 Doris 表 T_ODS_HSD_IPS 的实体类
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/15  17:07
 */
public class TOdsHsdIPSEntity {
    @JsonProperty("EVENT")
    private String EVENT;
    @JsonProperty("SUBEVENT")
    private String SUBEVENT;
    @JsonProperty("UPTM")
    private String UPTM;
    @JsonProperty("CREATETIME")
    private String CREATETIME;
    @JsonProperty("UPTATETIME")
    private String UPTATETIME;
    @JsonProperty("GDSRECORDLOCATOR")
    private String GDSRECORDLOCATOR;
    @JsonProperty("RECORDLOCATOR")
    private String RECORDLOCATOR;
    @JsonProperty("BOOKDATE")
    private Date BOOKDATE;
    @JsonProperty("TICKETNUMBER")
    private String TICKETNUMBER;
    @JsonProperty("EMDTICKETNUMBER")
    private String EMDTICKETNUMBER;
    @JsonProperty("CONTENT")
    private String CONTENT;
    @JsonProperty("CREATEDATE")
    private Date CREATEDATE;

    // Getter & Setter 方法
    public String getEVENT() { return EVENT; }
    public void setEVENT(String EVENT) { this.EVENT = EVENT; }

    public String getSUBEVENT() { return SUBEVENT; }
    public void setSUBEVENT(String SUBEVENT) { this.SUBEVENT = SUBEVENT; }

    public String getUPTM() { return UPTM; }
    public void setUPTM(String UPTM) { this.UPTM = UPTM; }

    public String getCREATETIME() { return CREATETIME; }
    public void setCREATETIME(String CREATETIME) { this.CREATETIME = CREATETIME; }

    public String getUPTATETIME() { return UPTATETIME; }
    public void setUPTATETIME(String UPTATETIME) { this.UPTATETIME = UPTATETIME; }

    public String getGDSRECORDLOCATOR() { return GDSRECORDLOCATOR; }
    public void setGDSRECORDLOCATOR(String GDSRECORDLOCATOR) { this.GDSRECORDLOCATOR = GDSRECORDLOCATOR; }

    public String getRECORDLOCATOR() { return RECORDLOCATOR; }
    public void setRECORDLOCATOR(String RECORDLOCATOR) { this.RECORDLOCATOR = RECORDLOCATOR; }

    public Date getBOOKDATE() { return BOOKDATE; }
    public void setBOOKDATE(Date BOOKDATE) { this.BOOKDATE = BOOKDATE; }

    public String getTICKETNUMBER() { return TICKETNUMBER; }
    public void setTICKETNUMBER(String TICKETNUMBER) { this.TICKETNUMBER = TICKETNUMBER; }

    public String getEMDTICKETNUMBER() { return EMDTICKETNUMBER; }
    public void setEMDTICKETNUMBER(String EMDTICKETNUMBER) { this.EMDTICKETNUMBER = EMDTICKETNUMBER; }

    public String getCONTENT() { return CONTENT; }
    public void setCONTENT(String CONTENT) { this.CONTENT = CONTENT; }

    public Date getCREATEDATE() { return CREATEDATE; }
    public void setCREATEDATE(Date CREATEDATE) { this.CREATEDATE = CREATEDATE; }

    @Override
    public String toString() {
        return "{"
                + "TOdsHsdIPSEntity: {"
                + "EVENT: " + EVENT
                + ", " + "SUBEVENT: " + SUBEVENT
                + ", " + "UPTM: " + UPTM
                + ", " + "CREATETIME: " + CREATETIME
                + ", " + "UPTATETIME: " + UPTATETIME
                + ", " + "GDSRECORDLOCATOR: " + GDSRECORDLOCATOR
                + ", " + "RECORDLOCATOR: " + RECORDLOCATOR
                + ", " + "BOOKDATE: " + BOOKDATE
                + ", " + "TICKETNUMBER: " + TICKETNUMBER
                + ", " + "EMDTICKETNUMBER: " + EMDTICKETNUMBER
                + ", " + "CONTENT: " + CONTENT
                + ", " + "CREATEDATE: " + CREATEDATE
                + "}"
                + "}";
    }
}
