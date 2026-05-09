package com.travelsky.dataplatform.module.ods;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * @author
 * @date 2025/7/7 17:47
 */
public class TOdsClkExchint {

    public TOdsClkExchint(TOdsClkExchint dto) {
        this.DOCNAME = dto.getDOCNAME();
        this.TOTALRECORD = dto.getTOTALRECORD();
        this.TOTALAMOUNT = dto.getTOTALAMOUNT();
        this.TOTALVALUE = dto.getTOTALVALUE();
        this.BILLINGMONTH = dto.getBILLINGMONTH();
        this.BILLINGSTARTDT = dto.getBILLINGSTARTDT();
        this.BILLINGENDDT = dto.getBILLINGENDDT();
        this.etlDate = LocalDate.now().toString();
    }

    public TOdsClkExchint() {
    }

    @JsonProperty("ETL_DATE")
    private String etlDate;

    @JsonProperty("ID")
    private Long ID;
    @JsonProperty("DOCNAME")
    private String DOCNAME;

    @JsonProperty("TOTALRECORD")
    private Long TOTALRECORD;

    @JsonProperty("TOTALAMOUNT")
    private Long TOTALAMOUNT;

    @JsonProperty("TOTALVALUE")
    private BigDecimal TOTALVALUE;

    @JsonProperty("BILLINGMONTH")
    private String BILLINGMONTH;

    @JsonProperty("BILLINGSTARTDT")
    private String BILLINGSTARTDT;

    @JsonProperty("BILLINGENDDT")
    private String BILLINGENDDT;

    @JsonProperty("MEMBERNO")
    private String MEMBERNO;

    @JsonProperty("MEMBERTIERCODE")
    private String MEMBERTIERCODE;

    @JsonProperty("MEMBERBRAND")
    private String MEMBERBRAND;

    @JsonProperty("BIZTYPECODE")
    private String BIZTYPECODE;

    @JsonProperty("BIZSUBTYPECODE")
    private String BIZSUBTYPECODE;

    @JsonProperty("EXCHCHANNEL")
    private String EXCHCHANNEL;

    @JsonProperty("PARTNERCODE")
    private String PARTNERCODE;

    @JsonProperty("EXCHNO")
    private String EXCHNO;

    @JsonProperty("ACTIVITYID")
    private String ACTIVITYID;

    @JsonProperty("TRANID")
    private String TRANID;

    @JsonProperty("ORDERNO")
    private String ORDERNO;

    @JsonProperty("TKTNO")
    private String TKTNO;

    @JsonProperty("COUPONNO")
    private String COUPONNO;

    @JsonProperty("BILLCOMPANY")
    private String BILLCOMPANY;

    @JsonProperty("BILLEDCOMPANY")
    private String BILLEDCOMPANY;

    @JsonProperty("MILES")
    private Long MILES;

    @JsonProperty("VALUE")
    private BigDecimal VALUE;

    @JsonProperty("TPM")
    private String TPM;

    @JsonProperty("CURRENCY")
    private String CURRENCY;

    @JsonProperty("SALESPRICE")
    private BigDecimal SALESPRICE;

    @JsonProperty("COSTSPRICE")
    private BigDecimal COSTSPRICE;

    @JsonProperty("PNR")
    private String PNR;

    @JsonProperty("EXCHDATE")
    private String EXCHDATE;

    @JsonProperty("FLIGHTDATE")
    private String FLIGHTDATE;

    @JsonProperty("OC")
    private String OC;

    @JsonProperty("OCFLIGHTNO")
    private String OCFLIGHTNO;

    @JsonProperty("OCCABIN")
    private String OCCABIN;

    @JsonProperty("OCSUBCLASS")
    private String OCSUBCLASS;

    @JsonProperty("BUSUBCLASS")
    private String BUSUBCLASS;

    @JsonProperty("UPLSTN")
    private String UPLSTN;

    @JsonProperty("DESSTN")
    private String DESSTN;

    @JsonProperty("COMMODITYNO")
    private String COMMODITYNO;

    @JsonProperty("PRODCUINFO")
    private String PRODCUINFO;

    @JsonProperty("ASS")
    private String ASS;

    @Override
    public String toString() {
        return "TOdsClkExchint{" +
                "etlDate='" + etlDate + '\'' +
                ", ID=" + ID +
                ", DOCNAME='" + DOCNAME + '\'' +
                ", TOTALRECORD=" + TOTALRECORD +
                ", TOTALAMOUNT=" + TOTALAMOUNT +
                ", TOTALVALUE=" + TOTALVALUE +
                ", BILLINGMONTH='" + BILLINGMONTH + '\'' +
                ", BILLINGSTARTDT='" + BILLINGSTARTDT + '\'' +
                ", BILLINGENDDT='" + BILLINGENDDT + '\'' +
                ", MEMBERNO='" + MEMBERNO + '\'' +
                ", MEMBERTIERCODE='" + MEMBERTIERCODE + '\'' +
                ", MEMBERBRAND='" + MEMBERBRAND + '\'' +
                ", BIZTYPECODE='" + BIZTYPECODE + '\'' +
                ", BIZSUBTYPECODE='" + BIZSUBTYPECODE + '\'' +
                ", EXCHCHANNEL='" + EXCHCHANNEL + '\'' +
                ", PARTNERCODE='" + PARTNERCODE + '\'' +
                ", EXCHNO='" + EXCHNO + '\'' +
                ", ACTIVITYID='" + ACTIVITYID + '\'' +
                ", TRANID='" + TRANID + '\'' +
                ", ORDERNO='" + ORDERNO + '\'' +
                ", TKTNO='" + TKTNO + '\'' +
                ", COUPONNO='" + COUPONNO + '\'' +
                ", BILLCOMPANY='" + BILLCOMPANY + '\'' +
                ", BILLEDCOMPANY='" + BILLEDCOMPANY + '\'' +
                ", MILES=" + MILES +
                ", VALUE=" + VALUE +
                ", TPM='" + TPM + '\'' +
                ", CURRENCY='" + CURRENCY + '\'' +
                ", SALESPRICE=" + SALESPRICE +
                ", COSTSPRICE=" + COSTSPRICE +
                ", PNR='" + PNR + '\'' +
                ", EXCHDATE='" + EXCHDATE + '\'' +
                ", FLIGHTDATE='" + FLIGHTDATE + '\'' +
                ", OC='" + OC + '\'' +
                ", OCFLIGHTNO='" + OCFLIGHTNO + '\'' +
                ", OCCABIN='" + OCCABIN + '\'' +
                ", OCSUBCLASS='" + OCSUBCLASS + '\'' +
                ", BUSUBCLASS='" + BUSUBCLASS + '\'' +
                ", UPLSTN='" + UPLSTN + '\'' +
                ", DESSTN='" + DESSTN + '\'' +
                ", COMMODITYNO='" + COMMODITYNO + '\'' +
                ", PRODCUINFO='" + PRODCUINFO + '\'' +
                ", ASS='" + ASS + '\'' +
                '}';
    }

    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public String getDOCNAME() {
        return DOCNAME;
    }

    public void setDOCNAME(String DOCNAME) {
        this.DOCNAME = DOCNAME;
    }

    public Long getTOTALRECORD() {
        return TOTALRECORD;
    }

    public void setTOTALRECORD(Long TOTALRECORD) {
        this.TOTALRECORD = TOTALRECORD;
    }

    public Long getTOTALAMOUNT() {
        return TOTALAMOUNT;
    }

    public void setTOTALAMOUNT(Long TOTALAMOUNT) {
        this.TOTALAMOUNT = TOTALAMOUNT;
    }


    public String getBILLINGMONTH() {
        return BILLINGMONTH;
    }

    public void setBILLINGMONTH(String BILLINGMONTH) {
        this.BILLINGMONTH = BILLINGMONTH;
    }

    public String getBILLINGSTARTDT() {
        return BILLINGSTARTDT;
    }

    public void setBILLINGSTARTDT(String BILLINGSTARTDT) {
        this.BILLINGSTARTDT = BILLINGSTARTDT;
    }

    public String getBILLINGENDDT() {
        return BILLINGENDDT;
    }

    public void setBILLINGENDDT(String BILLINGENDDT) {
        this.BILLINGENDDT = BILLINGENDDT;
    }

    public String getMEMBERNO() {
        return MEMBERNO;
    }

    public void setMEMBERNO(String MEMBERNO) {
        this.MEMBERNO = MEMBERNO;
    }

    public String getMEMBERTIERCODE() {
        return MEMBERTIERCODE;
    }

    public void setMEMBERTIERCODE(String MEMBERTIERCODE) {
        this.MEMBERTIERCODE = MEMBERTIERCODE;
    }

    public String getMEMBERBRAND() {
        return MEMBERBRAND;
    }

    public void setMEMBERBRAND(String MEMBERBRAND) {
        this.MEMBERBRAND = MEMBERBRAND;
    }

    public String getBIZTYPECODE() {
        return BIZTYPECODE;
    }

    public void setBIZTYPECODE(String BIZTYPECODE) {
        this.BIZTYPECODE = BIZTYPECODE;
    }

    public String getBIZSUBTYPECODE() {
        return BIZSUBTYPECODE;
    }

    public void setBIZSUBTYPECODE(String BIZSUBTYPECODE) {
        this.BIZSUBTYPECODE = BIZSUBTYPECODE;
    }

    public String getEXCHCHANNEL() {
        return EXCHCHANNEL;
    }

    public void setEXCHCHANNEL(String EXCHCHANNEL) {
        this.EXCHCHANNEL = EXCHCHANNEL;
    }

    public String getPARTNERCODE() {
        return PARTNERCODE;
    }

    public void setPARTNERCODE(String PARTNERCODE) {
        this.PARTNERCODE = PARTNERCODE;
    }

    public String getEXCHNO() {
        return EXCHNO;
    }

    public void setEXCHNO(String EXCHNO) {
        this.EXCHNO = EXCHNO;
    }

    public String getACTIVITYID() {
        return ACTIVITYID;
    }

    public void setACTIVITYID(String ACTIVITYID) {
        this.ACTIVITYID = ACTIVITYID;
    }

    public String getTRANID() {
        return TRANID;
    }

    public void setTRANID(String TRANID) {
        this.TRANID = TRANID;
    }

    public String getORDERNO() {
        return ORDERNO;
    }

    public void setORDERNO(String ORDERNO) {
        this.ORDERNO = ORDERNO;
    }

    public String getTKTNO() {
        return TKTNO;
    }

    public void setTKTNO(String TKTNO) {
        this.TKTNO = TKTNO;
    }

    public String getCOUPONNO() {
        return COUPONNO;
    }

    public void setCOUPONNO(String COUPONNO) {
        this.COUPONNO = COUPONNO;
    }

    public String getBILLCOMPANY() {
        return BILLCOMPANY;
    }

    public void setBILLCOMPANY(String BILLCOMPANY) {
        this.BILLCOMPANY = BILLCOMPANY;
    }

    public String getBILLEDCOMPANY() {
        return BILLEDCOMPANY;
    }

    public void setBILLEDCOMPANY(String BILLEDCOMPANY) {
        this.BILLEDCOMPANY = BILLEDCOMPANY;
    }

    public Long getMILES() {
        return MILES;
    }

    public void setMILES(Long MILES) {
        this.MILES = MILES;
    }

    public String getCURRENCY() {
        return CURRENCY;
    }

    public void setCURRENCY(String CURRENCY) {
        this.CURRENCY = CURRENCY;
    }

    public BigDecimal getTOTALVALUE() {
        return TOTALVALUE;
    }

    public void setTOTALVALUE(BigDecimal TOTALVALUE) {
        this.TOTALVALUE = TOTALVALUE;
    }

    public BigDecimal getSALESPRICE() {
        return SALESPRICE;
    }

    public void setSALESPRICE(BigDecimal SALESPRICE) {
        this.SALESPRICE = SALESPRICE;
    }

    public BigDecimal getCOSTSPRICE() {
        return COSTSPRICE;
    }

    public void setCOSTSPRICE(BigDecimal COSTSPRICE) {
        this.COSTSPRICE = COSTSPRICE;
    }

    public BigDecimal getVALUE() {
        return VALUE;
    }

    public void setVALUE(BigDecimal VALUE) {
        this.VALUE = VALUE;
    }


    public String getOC() {
        return OC;
    }

    public void setOC(String OC) {
        this.OC = OC;
    }

    public String getOCFLIGHTNO() {
        return OCFLIGHTNO;
    }

    public void setOCFLIGHTNO(String OCFLIGHTNO) {
        this.OCFLIGHTNO = OCFLIGHTNO;
    }

    public String getOCCABIN() {
        return OCCABIN;
    }

    public void setOCCABIN(String OCCABIN) {
        this.OCCABIN = OCCABIN;
    }

    public String getOCSUBCLASS() {
        return OCSUBCLASS;
    }

    public void setOCSUBCLASS(String OCSUBCLASS) {
        this.OCSUBCLASS = OCSUBCLASS;
    }

    public String getUPLSTN() {
        return UPLSTN;
    }

    public void setUPLSTN(String UPLSTN) {
        this.UPLSTN = UPLSTN;
    }

    public String getDESSTN() {
        return DESSTN;
    }

    public void setDESSTN(String DESSTN) {
        this.DESSTN = DESSTN;
    }

    public String getASS() {
        return ASS;
    }

    public void setASS(String ASS) {
        this.ASS = ASS;
    }

    public String getTPM() {
        return TPM;
    }

    public void setTPM(String TPM) {
        this.TPM = TPM;
    }

    public String getPNR() {
        return PNR;
    }

    public void setPNR(String PNR) {
        this.PNR = PNR;
    }

    public String getEXCHDATE() {
        return EXCHDATE;
    }

    public void setEXCHDATE(String EXCHDATE) {
        this.EXCHDATE = EXCHDATE;
    }

    public String getFLIGHTDATE() {
        return FLIGHTDATE;
    }

    public void setFLIGHTDATE(String FLIGHTDATE) {
        this.FLIGHTDATE = FLIGHTDATE;
    }

    public String getBUSUBCLASS() {
        return BUSUBCLASS;
    }

    public void setBUSUBCLASS(String BUSUBCLASS) {
        this.BUSUBCLASS = BUSUBCLASS;
    }

    public String getCOMMODITYNO() {
        return COMMODITYNO;
    }

    public void setCOMMODITYNO(String COMMODITYNO) {
        this.COMMODITYNO = COMMODITYNO;
    }

    public String getPRODCUINFO() {
        return PRODCUINFO;
    }

    public void setPRODCUINFO(String PRODCUINFO) {
        this.PRODCUINFO = PRODCUINFO;
    }

    public String getEtlDate() {
        return etlDate;
    }

    public void setEtlDate(String etlDate) {
        this.etlDate = etlDate;
    }
}
