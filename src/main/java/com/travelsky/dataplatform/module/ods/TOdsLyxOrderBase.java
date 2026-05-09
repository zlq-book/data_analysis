package com.travelsky.dataplatform.module.ods;

import com.fasterxml.jackson.annotation.JsonProperty;


public class TOdsLyxOrderBase {
    @JsonProperty("ID")
    private Long ID;

    @JsonProperty("ORDER_NO")
    private String ORDER_NO;
    @JsonProperty("ORDER_STATUS")
    private String ORDER_STATUS;
    @JsonProperty("ORDER_TYPE")
    private String ORDER_TYPE;
    @JsonProperty("PRODUCT_NAME")
    private String PRODUCT_NAME;
    @JsonProperty("ORDER_TIME")
    private Long ORDER_TIME;
    @JsonProperty("ORDER_ORIGIN")
    private String ORDER_ORIGIN;
    @JsonProperty("CURRENT_DEALER")
    private String CURRENT_DEALER;
    @JsonProperty("DEAL_TIME")
    private Long DEAL_TIME;
    @JsonProperty("PSG_NAME")
    private String PSG_NAME;
    @JsonProperty("VIP_IF")
    private String VIP_IF;
    @JsonProperty("VIP_LEVEL")
    private String VIP_LEVEL;
    @JsonProperty("VIP_TYPE")
    private String VIP_TYPE;
    @JsonProperty("CERTIFY_TYPE")
    private String CERTIFY_TYPE;
    @JsonProperty("CERTIFY_NUM")
    private String CERTIFY_NUM;
    @JsonProperty("PHONE_NUM")
    private String PHONE_NUM;
    @JsonProperty("TICKET_NUM")
    private String TICKET_NUM;
    @JsonProperty("TICKET_STATUS")
    private String TICKET_STATUS;
    @JsonProperty("ISSUE_OFFICE")
    private String ISSUE_OFFICE;
    @JsonProperty("BOOK_OFFICE")
    private String BOOK_OFFICE;
    @JsonProperty("FLT_NUM")
    private String FLT_NUM;
    @JsonProperty("FLT_DATE")
    private Long FLT_DATE;
    @JsonProperty("ORIG")
    private String ORIG;
    @JsonProperty("DEST")
    private String DEST;
    @JsonProperty("LAUNCH_TIME")
    private Long LAUNCH_TIME;
    @JsonProperty("ARRIVE_TIME")
    private Long ARRIVE_TIME;
    @JsonProperty("FLT_STATUS")
    private String FLT_STATUS;
    @JsonProperty("TICKET_PRICE")
    private String TICKET_PRICE;
    @JsonProperty("CABIN_CLASS")
    private String CABIN_CLASS;
    @JsonProperty("DISCUSS_STATUS")
    private String DISCUSS_STATUS;
    @JsonProperty("DISCUSS_CONTENT")
    private String DISCUSS_CONTENT;
    @JsonProperty("DISCUSS_LEVEL")
    private String DISCUSS_LEVEL;
    @JsonProperty("DISCUSS_TIME")
    private Long DISCUSS_TIME;
    @JsonProperty("CREATOR")
    private String CREATOR;
    @JsonProperty("CREATE_TIME")
    private Long CREATE_TIME;
    @JsonProperty("UPDATOR")
    private String UPDATOR;
    @JsonProperty("UPDATE_TIME")
    private Long UPDATE_TIME;
    @JsonProperty("TICKET_FLAG")
    private String TICKET_FLAG;
    @JsonProperty("ORDER_OLD_NO")
    private String ORDER_OLD_NO;
    @JsonProperty("PRODUCT_CODE")
    private String PRODUCT_CODE;
    @JsonProperty("PRODUCT_ID")
    private Long PRODUCT_ID;
    @JsonProperty("ORIG_NAME")
    private String ORIG_NAME;
    @JsonProperty("DEST_NAME")
    private String DEST_NAME;
    @JsonProperty("PSG_ID")
    private Long PSG_ID;
    @JsonProperty("ORIG_TICKET_NUM")
    private String ORIG_TICKET_NUM;
    @JsonProperty("ORIG_TICKET_PRICE")
    private String ORIG_TICKET_PRICE;
    @JsonProperty("ORIG_FLT_NUM")
    private String ORIG_FLT_NUM;
    @JsonProperty("ORIG_FLT_DATE")
    private Long ORIG_FLT_DATE;
    @JsonProperty("ORIG_ORIG")
    private String ORIG_ORIG;
    @JsonProperty("ORIG_DEST")
    private String ORIG_DEST;
    @JsonProperty("ORIG_LAUNCH_TIME")
    private Long ORIG_LAUNCH_TIME;
    @JsonProperty("ORIG_ARRIVE_TIME")
    private Long ORIG_ARRIVE_TIME;
    @JsonProperty("ORIG_CABIN_CLASS")
    private String ORIG_CABIN_CLASS;
    @JsonProperty("GROUP_ID")
    private Long GROUP_ID;
    @JsonProperty("PSG_CARDNO")
    private String PSG_CARDNO;
    @JsonProperty("OUT_CLASS")
    private String OUT_CLASS;
    @JsonProperty("OUT_PRICE")
    private String OUT_PRICE;
    @JsonProperty("ADDPRICE_INCOME")
    private String ADDPRICE_INCOME;
    @JsonProperty("LINK_ID")
    private String LINK_ID;
    @JsonProperty("LINK_SOURCE")
    private String LINK_SOURCE;
    @JsonProperty("PNR_ICS")
    private String PNR_ICS;
    @JsonProperty("PNR_CRS")
    private String PNR_CRS;
    @JsonProperty("LINK_FLAG")
    private String LINK_FLAG;
    @JsonProperty("IS_REVALIDATION")
    private String IS_REVALIDATION;
    @JsonProperty("PAY_OPERATE_REASON")
    private String PAY_OPERATE_REASON;
    @JsonProperty("PAY_OPERATE_TYPE")
    private String PAY_OPERATE_TYPE;
    @JsonProperty("REMIND_FLAG")
    private String REMIND_FLAG;
    @JsonProperty("IATA_NO")
    private String IATA_NO;
    @JsonProperty("AGENT_NAME")
    private String AGENT_NAME;
    @JsonProperty("SERVICE_COST")
    private String SERVICE_COST;
    @JsonProperty("PAY_STATUS")
    private String PAY_STATUS;
    @JsonProperty("SALES_DEPT")
    private String SALES_DEPT;
    @JsonProperty("B2B_OFFICE")
    private String B2B_OFFICE;
    @JsonProperty("BI_DEPTNAME")
    private String BI_DEPTNAME;
    @JsonProperty("BI_SERVICE_COST")
    private String BI_SERVICE_COST;
    @JsonProperty("BI_PROMOTION_COST")
    private String BI_PROMOTION_COST;
    @JsonProperty("SERVICE_LEVEL")
    private String SERVICE_LEVEL;
    @JsonProperty("ISSUE_TIME")
    private Long ISSUE_TIME;
    @JsonProperty("BI_MARKETING_INCENTIVES_PRICE")
    private String BI_MARKETING_INCENTIVES_PRICE;

    @Override
    public String toString() {
        return "TOdsLyxOrderBase{" +
                "ID=" + ID +
                ", ORDER_NO='" + ORDER_NO + '\'' +
                ", ORDER_STATUS='" + ORDER_STATUS + '\'' +
                ", ORDER_TYPE='" + ORDER_TYPE + '\'' +
                ", PRODUCT_NAME='" + PRODUCT_NAME + '\'' +
                ", ORDER_TIME='" + ORDER_TIME + '\'' +
                ", ORDER_ORIGIN='" + ORDER_ORIGIN + '\'' +
                ", CURRENT_DEALER='" + CURRENT_DEALER + '\'' +
                ", DEAL_TIME='" + DEAL_TIME + '\'' +
                ", PSG_NAME='" + PSG_NAME + '\'' +
                ", VIP_IF='" + VIP_IF + '\'' +
                ", VIP_LEVEL='" + VIP_LEVEL + '\'' +
                ", VIP_TYPE='" + VIP_TYPE + '\'' +
                ", CERTIFY_TYPE='" + CERTIFY_TYPE + '\'' +
                ", CERTIFY_NUM='" + CERTIFY_NUM + '\'' +
                ", PHONE_NUM='" + PHONE_NUM + '\'' +
                ", TICKET_NUM='" + TICKET_NUM + '\'' +
                ", TICKET_STATUS='" + TICKET_STATUS + '\'' +
                ", ISSUE_OFFICE='" + ISSUE_OFFICE + '\'' +
                ", BOOK_OFFICE='" + BOOK_OFFICE + '\'' +
                ", FLT_NUM='" + FLT_NUM + '\'' +
                ", FLT_DATE='" + FLT_DATE + '\'' +
                ", ORIG='" + ORIG + '\'' +
                ", DEST='" + DEST + '\'' +
                ", LAUNCH_TIME='" + LAUNCH_TIME + '\'' +
                ", ARRIVE_TIME='" + ARRIVE_TIME + '\'' +
                ", FLT_STATUS='" + FLT_STATUS + '\'' +
                ", TICKET_PRICE='" + TICKET_PRICE + '\'' +
                ", CABIN_CLASS='" + CABIN_CLASS + '\'' +
                ", DISCUSS_STATUS='" + DISCUSS_STATUS + '\'' +
                ", DISCUSS_CONTENT='" + DISCUSS_CONTENT + '\'' +
                ", DISCUSS_LEVEL='" + DISCUSS_LEVEL + '\'' +
                ", DISCUSS_TIME='" + DISCUSS_TIME + '\'' +
                ", CREATOR='" + CREATOR + '\'' +
                ", CREATE_TIME='" + CREATE_TIME + '\'' +
                ", UPDATOR='" + UPDATOR + '\'' +
                ", UPDATE_TIME='" + UPDATE_TIME + '\'' +
                ", TICKET_FLAG='" + TICKET_FLAG + '\'' +
                ", ORDER_OLD_NO='" + ORDER_OLD_NO + '\'' +
                ", PRODUCT_CODE='" + PRODUCT_CODE + '\'' +
                ", PRODUCT_ID=" + PRODUCT_ID +
                ", ORIG_NAME='" + ORIG_NAME + '\'' +
                ", DEST_NAME='" + DEST_NAME + '\'' +
                ", PSG_ID=" + PSG_ID +
                ", ORIG_TICKET_NUM='" + ORIG_TICKET_NUM + '\'' +
                ", ORIG_TICKET_PRICE='" + ORIG_TICKET_PRICE + '\'' +
                ", ORIG_FLT_NUM='" + ORIG_FLT_NUM + '\'' +
                ", ORIG_FLT_DATE='" + ORIG_FLT_DATE + '\'' +
                ", ORIG_ORIG='" + ORIG_ORIG + '\'' +
                ", ORIG_DEST='" + ORIG_DEST + '\'' +
                ", ORIG_LAUNCH_TIME='" + ORIG_LAUNCH_TIME + '\'' +
                ", ORIG_ARRIVE_TIME='" + ORIG_ARRIVE_TIME + '\'' +
                ", ORIG_CABIN_CLASS='" + ORIG_CABIN_CLASS + '\'' +
                ", GROUP_ID=" + GROUP_ID +
                ", PSG_CARDNO='" + PSG_CARDNO + '\'' +
                ", OUT_CLASS='" + OUT_CLASS + '\'' +
                ", OUT_PRICE='" + OUT_PRICE + '\'' +
                ", ADDPRICE_INCOME='" + ADDPRICE_INCOME + '\'' +
                ", LINK_ID='" + LINK_ID + '\'' +
                ", LINK_SOURCE='" + LINK_SOURCE + '\'' +
                ", PNR_ICS='" + PNR_ICS + '\'' +
                ", PNR_CRS='" + PNR_CRS + '\'' +
                ", LINK_FLAG='" + LINK_FLAG + '\'' +
                ", IS_REVALIDATION='" + IS_REVALIDATION + '\'' +
                ", PAY_OPERATE_REASON='" + PAY_OPERATE_REASON + '\'' +
                ", PAY_OPERATE_TYPE='" + PAY_OPERATE_TYPE + '\'' +
                ", REMIND_FLAG='" + REMIND_FLAG + '\'' +
                ", IATA_NO='" + IATA_NO + '\'' +
                ", AGENT_NAME='" + AGENT_NAME + '\'' +
                ", SERVICE_COST='" + SERVICE_COST + '\'' +
                ", PAY_STATUS='" + PAY_STATUS + '\'' +
                ", SALES_DEPT='" + SALES_DEPT + '\'' +
                ", B2B_OFFICE='" + B2B_OFFICE + '\'' +
                ", BI_DEPTNAME='" + BI_DEPTNAME + '\'' +
                ", BI_SERVICE_COST='" + BI_SERVICE_COST + '\'' +
                ", BI_PROMOTION_COST='" + BI_PROMOTION_COST + '\'' +
                ", SERVICE_LEVEL='" + SERVICE_LEVEL + '\'' +
                ", ISSUE_TIME='" + ISSUE_TIME + '\'' +
                ", BI_MARKETING_INCENTIVES_PRICE='" + BI_MARKETING_INCENTIVES_PRICE + '\'' +
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

    public String getORDER_STATUS() {
        return ORDER_STATUS;
    }

    public void setORDER_STATUS(String ORDER_STATUS) {
        this.ORDER_STATUS = ORDER_STATUS;
    }

    public String getORDER_TYPE() {
        return ORDER_TYPE;
    }

    public void setORDER_TYPE(String ORDER_TYPE) {
        this.ORDER_TYPE = ORDER_TYPE;
    }

    public String getPRODUCT_NAME() {
        return PRODUCT_NAME;
    }

    public void setPRODUCT_NAME(String PRODUCT_NAME) {
        this.PRODUCT_NAME = PRODUCT_NAME;
    }



    public String getORDER_ORIGIN() {
        return ORDER_ORIGIN;
    }

    public void setORDER_ORIGIN(String ORDER_ORIGIN) {
        this.ORDER_ORIGIN = ORDER_ORIGIN;
    }

    public String getCURRENT_DEALER() {
        return CURRENT_DEALER;
    }

    public void setCURRENT_DEALER(String CURRENT_DEALER) {
        this.CURRENT_DEALER = CURRENT_DEALER;
    }



    public String getPSG_NAME() {
        return PSG_NAME;
    }

    public void setPSG_NAME(String PSG_NAME) {
        this.PSG_NAME = PSG_NAME;
    }

    public String getVIP_IF() {
        return VIP_IF;
    }

    public void setVIP_IF(String VIP_IF) {
        this.VIP_IF = VIP_IF;
    }

    public String getVIP_LEVEL() {
        return VIP_LEVEL;
    }

    public void setVIP_LEVEL(String VIP_LEVEL) {
        this.VIP_LEVEL = VIP_LEVEL;
    }

    public String getVIP_TYPE() {
        return VIP_TYPE;
    }

    public void setVIP_TYPE(String VIP_TYPE) {
        this.VIP_TYPE = VIP_TYPE;
    }

    public String getCERTIFY_TYPE() {
        return CERTIFY_TYPE;
    }

    public void setCERTIFY_TYPE(String CERTIFY_TYPE) {
        this.CERTIFY_TYPE = CERTIFY_TYPE;
    }

    public String getCERTIFY_NUM() {
        return CERTIFY_NUM;
    }

    public void setCERTIFY_NUM(String CERTIFY_NUM) {
        this.CERTIFY_NUM = CERTIFY_NUM;
    }

    public String getPHONE_NUM() {
        return PHONE_NUM;
    }

    public void setPHONE_NUM(String PHONE_NUM) {
        this.PHONE_NUM = PHONE_NUM;
    }

    public String getTICKET_NUM() {
        return TICKET_NUM;
    }

    public void setTICKET_NUM(String TICKET_NUM) {
        this.TICKET_NUM = TICKET_NUM;
    }

    public String getTICKET_STATUS() {
        return TICKET_STATUS;
    }

    public void setTICKET_STATUS(String TICKET_STATUS) {
        this.TICKET_STATUS = TICKET_STATUS;
    }

    public String getISSUE_OFFICE() {
        return ISSUE_OFFICE;
    }

    public void setISSUE_OFFICE(String ISSUE_OFFICE) {
        this.ISSUE_OFFICE = ISSUE_OFFICE;
    }

    public String getBOOK_OFFICE() {
        return BOOK_OFFICE;
    }

    public void setBOOK_OFFICE(String BOOK_OFFICE) {
        this.BOOK_OFFICE = BOOK_OFFICE;
    }

    public String getFLT_NUM() {
        return FLT_NUM;
    }

    public void setFLT_NUM(String FLT_NUM) {
        this.FLT_NUM = FLT_NUM;
    }



    public String getORIG() {
        return ORIG;
    }

    public void setORIG(String ORIG) {
        this.ORIG = ORIG;
    }

    public String getDEST() {
        return DEST;
    }

    public void setDEST(String DEST) {
        this.DEST = DEST;
    }



    public String getFLT_STATUS() {
        return FLT_STATUS;
    }

    public void setFLT_STATUS(String FLT_STATUS) {
        this.FLT_STATUS = FLT_STATUS;
    }

    public String getTICKET_PRICE() {
        return TICKET_PRICE;
    }

    public void setTICKET_PRICE(String TICKET_PRICE) {
        this.TICKET_PRICE = TICKET_PRICE;
    }

    public String getCABIN_CLASS() {
        return CABIN_CLASS;
    }

    public void setCABIN_CLASS(String CABIN_CLASS) {
        this.CABIN_CLASS = CABIN_CLASS;
    }

    public String getDISCUSS_STATUS() {
        return DISCUSS_STATUS;
    }

    public void setDISCUSS_STATUS(String DISCUSS_STATUS) {
        this.DISCUSS_STATUS = DISCUSS_STATUS;
    }

    public String getDISCUSS_CONTENT() {
        return DISCUSS_CONTENT;
    }

    public void setDISCUSS_CONTENT(String DISCUSS_CONTENT) {
        this.DISCUSS_CONTENT = DISCUSS_CONTENT;
    }

    public String getDISCUSS_LEVEL() {
        return DISCUSS_LEVEL;
    }

    public void setDISCUSS_LEVEL(String DISCUSS_LEVEL) {
        this.DISCUSS_LEVEL = DISCUSS_LEVEL;
    }



    public String getCREATOR() {
        return CREATOR;
    }

    public void setCREATOR(String CREATOR) {
        this.CREATOR = CREATOR;
    }



    public String getUPDATOR() {
        return UPDATOR;
    }

    public void setUPDATOR(String UPDATOR) {
        this.UPDATOR = UPDATOR;
    }



    public String getTICKET_FLAG() {
        return TICKET_FLAG;
    }

    public void setTICKET_FLAG(String TICKET_FLAG) {
        this.TICKET_FLAG = TICKET_FLAG;
    }

    public String getORDER_OLD_NO() {
        return ORDER_OLD_NO;
    }

    public void setORDER_OLD_NO(String ORDER_OLD_NO) {
        this.ORDER_OLD_NO = ORDER_OLD_NO;
    }

    public String getPRODUCT_CODE() {
        return PRODUCT_CODE;
    }

    public void setPRODUCT_CODE(String PRODUCT_CODE) {
        this.PRODUCT_CODE = PRODUCT_CODE;
    }

    public Long getPRODUCT_ID() {
        return PRODUCT_ID;
    }

    public void setPRODUCT_ID(Long PRODUCT_ID) {
        this.PRODUCT_ID = PRODUCT_ID;
    }

    public String getORIG_NAME() {
        return ORIG_NAME;
    }

    public void setORIG_NAME(String ORIG_NAME) {
        this.ORIG_NAME = ORIG_NAME;
    }

    public String getDEST_NAME() {
        return DEST_NAME;
    }

    public void setDEST_NAME(String DEST_NAME) {
        this.DEST_NAME = DEST_NAME;
    }

    public Long getPSG_ID() {
        return PSG_ID;
    }

    public void setPSG_ID(Long PSG_ID) {
        this.PSG_ID = PSG_ID;
    }

    public String getORIG_TICKET_NUM() {
        return ORIG_TICKET_NUM;
    }

    public void setORIG_TICKET_NUM(String ORIG_TICKET_NUM) {
        this.ORIG_TICKET_NUM = ORIG_TICKET_NUM;
    }

    public String getORIG_TICKET_PRICE() {
        return ORIG_TICKET_PRICE;
    }

    public void setORIG_TICKET_PRICE(String ORIG_TICKET_PRICE) {
        this.ORIG_TICKET_PRICE = ORIG_TICKET_PRICE;
    }

    public String getORIG_FLT_NUM() {
        return ORIG_FLT_NUM;
    }

    public void setORIG_FLT_NUM(String ORIG_FLT_NUM) {
        this.ORIG_FLT_NUM = ORIG_FLT_NUM;
    }



    public String getORIG_ORIG() {
        return ORIG_ORIG;
    }

    public void setORIG_ORIG(String ORIG_ORIG) {
        this.ORIG_ORIG = ORIG_ORIG;
    }

    public String getORIG_DEST() {
        return ORIG_DEST;
    }

    public void setORIG_DEST(String ORIG_DEST) {
        this.ORIG_DEST = ORIG_DEST;
    }


    public String getORIG_CABIN_CLASS() {
        return ORIG_CABIN_CLASS;
    }

    public void setORIG_CABIN_CLASS(String ORIG_CABIN_CLASS) {
        this.ORIG_CABIN_CLASS = ORIG_CABIN_CLASS;
    }

    public Long getGROUP_ID() {
        return GROUP_ID;
    }

    public void setGROUP_ID(Long GROUP_ID) {
        this.GROUP_ID = GROUP_ID;
    }

    public String getPSG_CARDNO() {
        return PSG_CARDNO;
    }

    public void setPSG_CARDNO(String PSG_CARDNO) {
        this.PSG_CARDNO = PSG_CARDNO;
    }

    public String getOUT_CLASS() {
        return OUT_CLASS;
    }

    public void setOUT_CLASS(String OUT_CLASS) {
        this.OUT_CLASS = OUT_CLASS;
    }

    public String getOUT_PRICE() {
        return OUT_PRICE;
    }

    public void setOUT_PRICE(String OUT_PRICE) {
        this.OUT_PRICE = OUT_PRICE;
    }

    public String getADDPRICE_INCOME() {
        return ADDPRICE_INCOME;
    }

    public void setADDPRICE_INCOME(String ADDPRICE_INCOME) {
        this.ADDPRICE_INCOME = ADDPRICE_INCOME;
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

    public String getPNR_ICS() {
        return PNR_ICS;
    }

    public void setPNR_ICS(String PNR_ICS) {
        this.PNR_ICS = PNR_ICS;
    }

    public String getPNR_CRS() {
        return PNR_CRS;
    }

    public void setPNR_CRS(String PNR_CRS) {
        this.PNR_CRS = PNR_CRS;
    }

    public String getLINK_FLAG() {
        return LINK_FLAG;
    }

    public void setLINK_FLAG(String LINK_FLAG) {
        this.LINK_FLAG = LINK_FLAG;
    }

    public String getIS_REVALIDATION() {
        return IS_REVALIDATION;
    }

    public void setIS_REVALIDATION(String IS_REVALIDATION) {
        this.IS_REVALIDATION = IS_REVALIDATION;
    }

    public String getPAY_OPERATE_REASON() {
        return PAY_OPERATE_REASON;
    }

    public void setPAY_OPERATE_REASON(String PAY_OPERATE_REASON) {
        this.PAY_OPERATE_REASON = PAY_OPERATE_REASON;
    }

    public String getPAY_OPERATE_TYPE() {
        return PAY_OPERATE_TYPE;
    }

    public void setPAY_OPERATE_TYPE(String PAY_OPERATE_TYPE) {
        this.PAY_OPERATE_TYPE = PAY_OPERATE_TYPE;
    }

    public String getREMIND_FLAG() {
        return REMIND_FLAG;
    }

    public void setREMIND_FLAG(String REMIND_FLAG) {
        this.REMIND_FLAG = REMIND_FLAG;
    }

    public String getIATA_NO() {
        return IATA_NO;
    }

    public void setIATA_NO(String IATA_NO) {
        this.IATA_NO = IATA_NO;
    }

    public String getAGENT_NAME() {
        return AGENT_NAME;
    }

    public void setAGENT_NAME(String AGENT_NAME) {
        this.AGENT_NAME = AGENT_NAME;
    }

    public String getSERVICE_COST() {
        return SERVICE_COST;
    }

    public void setSERVICE_COST(String SERVICE_COST) {
        this.SERVICE_COST = SERVICE_COST;
    }

    public String getPAY_STATUS() {
        return PAY_STATUS;
    }

    public void setPAY_STATUS(String PAY_STATUS) {
        this.PAY_STATUS = PAY_STATUS;
    }

    public String getSALES_DEPT() {
        return SALES_DEPT;
    }

    public void setSALES_DEPT(String SALES_DEPT) {
        this.SALES_DEPT = SALES_DEPT;
    }

    public String getB2B_OFFICE() {
        return B2B_OFFICE;
    }

    public void setB2B_OFFICE(String b2B_OFFICE) {
        B2B_OFFICE = b2B_OFFICE;
    }

    public String getBI_DEPTNAME() {
        return BI_DEPTNAME;
    }

    public void setBI_DEPTNAME(String BI_DEPTNAME) {
        this.BI_DEPTNAME = BI_DEPTNAME;
    }

    public String getBI_SERVICE_COST() {
        return BI_SERVICE_COST;
    }

    public void setBI_SERVICE_COST(String BI_SERVICE_COST) {
        this.BI_SERVICE_COST = BI_SERVICE_COST;
    }

    public String getBI_PROMOTION_COST() {
        return BI_PROMOTION_COST;
    }

    public void setBI_PROMOTION_COST(String BI_PROMOTION_COST) {
        this.BI_PROMOTION_COST = BI_PROMOTION_COST;
    }

    public String getSERVICE_LEVEL() {
        return SERVICE_LEVEL;
    }

    public void setSERVICE_LEVEL(String SERVICE_LEVEL) {
        this.SERVICE_LEVEL = SERVICE_LEVEL;
    }

    public Long getORDER_TIME() {
        return ORDER_TIME;
    }

    public void setORDER_TIME(Long ORDER_TIME) {
        this.ORDER_TIME = ORDER_TIME;
    }

    public Long getDEAL_TIME() {
        return DEAL_TIME;
    }

    public void setDEAL_TIME(Long DEAL_TIME) {
        this.DEAL_TIME = DEAL_TIME;
    }

    public Long getFLT_DATE() {
        return FLT_DATE;
    }

    public void setFLT_DATE(Long FLT_DATE) {
        this.FLT_DATE = FLT_DATE;
    }

    public Long getLAUNCH_TIME() {
        return LAUNCH_TIME;
    }

    public void setLAUNCH_TIME(Long LAUNCH_TIME) {
        this.LAUNCH_TIME = LAUNCH_TIME;
    }

    public Long getARRIVE_TIME() {
        return ARRIVE_TIME;
    }

    public void setARRIVE_TIME(Long ARRIVE_TIME) {
        this.ARRIVE_TIME = ARRIVE_TIME;
    }

    public Long getDISCUSS_TIME() {
        return DISCUSS_TIME;
    }

    public void setDISCUSS_TIME(Long DISCUSS_TIME) {
        this.DISCUSS_TIME = DISCUSS_TIME;
    }

    public Long getCREATE_TIME() {
        return CREATE_TIME;
    }

    public void setCREATE_TIME(Long CREATE_TIME) {
        this.CREATE_TIME = CREATE_TIME;
    }

    public Long getUPDATE_TIME() {
        return UPDATE_TIME;
    }

    public void setUPDATE_TIME(Long UPDATE_TIME) {
        this.UPDATE_TIME = UPDATE_TIME;
    }

    public Long getORIG_FLT_DATE() {
        return ORIG_FLT_DATE;
    }

    public void setORIG_FLT_DATE(Long ORIG_FLT_DATE) {
        this.ORIG_FLT_DATE = ORIG_FLT_DATE;
    }

    public Long getORIG_LAUNCH_TIME() {
        return ORIG_LAUNCH_TIME;
    }

    public void setORIG_LAUNCH_TIME(Long ORIG_LAUNCH_TIME) {
        this.ORIG_LAUNCH_TIME = ORIG_LAUNCH_TIME;
    }

    public Long getORIG_ARRIVE_TIME() {
        return ORIG_ARRIVE_TIME;
    }

    public void setORIG_ARRIVE_TIME(Long ORIG_ARRIVE_TIME) {
        this.ORIG_ARRIVE_TIME = ORIG_ARRIVE_TIME;
    }

    public Long getISSUE_TIME() {
        return ISSUE_TIME;
    }

    public void setISSUE_TIME(Long ISSUE_TIME) {
        this.ISSUE_TIME = ISSUE_TIME;
    }

    public String getBI_MARKETING_INCENTIVES_PRICE() {
        return BI_MARKETING_INCENTIVES_PRICE;
    }

    public void setBI_MARKETING_INCENTIVES_PRICE(String BI_MARKETING_INCENTIVES_PRICE) {
        this.BI_MARKETING_INCENTIVES_PRICE = BI_MARKETING_INCENTIVES_PRICE;
    }
}
















































