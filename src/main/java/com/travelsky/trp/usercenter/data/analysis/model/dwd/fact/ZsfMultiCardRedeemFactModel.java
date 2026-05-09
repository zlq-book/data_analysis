package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 明细事实表-掌尚飞-次卡兑换机票
 */
public class ZsfMultiCardRedeemFactModel {

    /**
     * pk_id
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 关联订单号
     */
    @JsonProperty("AK_ORDERNUM")
    private String akOrdernum;

    /**
     * 产品编号
     */
    @JsonProperty("PROD_NO")
    private String prodNo;

    /**
     * 兑换的机票价格
     */
    @JsonProperty("FK_TKTPRICE")
    private BigDecimal fk_tktprice;

    public BigDecimal getFk_tktprice() {
        return fk_tktprice;
    }

    public void setFk_tktprice(BigDecimal fk_tktprice) {
        this.fk_tktprice = fk_tktprice;
    }

    /**
     * 出发机场
     */
    @JsonProperty("DEP_CITY")
    private String depcity;

    /**
     * 到达机场
     */
    @JsonProperty("ARR_CITY")
    private String arrcity;

    /**
     * 舱位
     */
    @JsonProperty("CABIN")
    private String cabin;

    /**
     * 乘机人中文姓名
     */
    @JsonProperty("CN_NAME")
    private String cnName;

    /**
     * 乘机人证件号
     */
    @JsonProperty("CERT_NO")
    private String certNo;

    /**
     * 乘机人
     */
    @JsonProperty("PASSENGER_USER")
    private String passengerUser;

    /**
     * 乘机人姓名
     */
    @JsonProperty("PSGR_NAME")
    private String psgrName;

    /**
     * 出发日期
     */
    @JsonProperty("DEP_DATE")
    private LocalDate depDate;

    /**
     * 出发时间
     */
    @JsonProperty("DEP_TIME")
    private String depTime;

    /**
     * 次卡编号
     */
    @JsonProperty("CARD_NO")
    private String cardNo;

    /**
     * 次卡抵用金额
     */
    @JsonProperty("CARD_AMOUNT")
    private BigDecimal cardAmount;

    /**
     * 次卡使用状态
     */
    @JsonProperty("CARD_PAYSTATUS")
    private String cardPaystatus;

    /**
     * 预订时间
     */
    @JsonProperty("FK_BOOKING_TIME")
    private String fkBookingTime;

    /**
     * 预订日期
     */
    @JsonProperty("FK_BOOKING_DATE")
    private String fkBookingDate;

    /**
     * 兑换次数计数
     */
    @JsonProperty("EXCH_COUNT")
    private Integer exchCount = 1;

    /**
     * 源系统最后更新时间（时间戳）
     */
    @JsonProperty("SOURCE_LAST_UPDATETIME")
    private String sourceLastUpdatetime = LocalDateTime.now().toString();

    /**
     * 本系统创建日期时间
     */
    @JsonProperty("SYSTEM_CREATETIME")
    private String systemCreatetime;

    /**
     * 本系统最后更新日期时间
     */
    @JsonProperty("SYSTEM_LAST_UPDATETIME")
    private String systemLastUpdatetime;

    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getAkOrdernum() {
        return akOrdernum;
    }

    public void setAkOrdernum(String akOrdernum) {
        this.akOrdernum = akOrdernum;
    }

    public String getProdNo() {
        return prodNo;
    }

    public void setProdNo(String prodNo) {
        this.prodNo = prodNo;
    }

    public String getDepcity() {
        return depcity;
    }

    public void setDepcity(String depcity) {
        this.depcity = depcity;
    }

    public String getArrcity() {
        return arrcity;
    }

    public void setArrcity(String arrcity) {
        this.arrcity = arrcity;
    }

    public String getCabin() {
        return cabin;
    }

    public void setCabin(String cabin) {
        this.cabin = cabin;
    }

    public String getCnName() {
        return cnName;
    }

    public void setCnName(String cnName) {
        this.cnName = cnName;
    }

    public String getCertNo() {
        return certNo;
    }

    public void setCertNo(String certNo) {
        this.certNo = certNo;
    }

    public String getPassengerUser() {
        return passengerUser;
    }

    public void setPassengerUser(String passengerUser) {
        this.passengerUser = passengerUser;
    }

    public String getPsgrName() {
        return psgrName;
    }

    public void setPsgrName(String psgrName) {
        this.psgrName = psgrName;
    }

    public LocalDate getDepDate() {
        return depDate;
    }

    public void setDepDate(LocalDate depDate) {
        this.depDate = depDate;
    }

    public String getDepTime() {
        return depTime;
    }

    public void setDepTime(String depTime) {
        this.depTime = depTime;
    }

    public String getCardNo() {
        return cardNo;
    }

    public void setCardNo(String cardNo) {
        this.cardNo = cardNo;
    }

    public BigDecimal getCardAmount() {
        return cardAmount;
    }

    public void setCardAmount(BigDecimal cardAmount) {
        this.cardAmount = cardAmount;
    }

    public String getCardPaystatus() {
        return cardPaystatus;
    }

    public void setCardPaystatus(String cardPaystatus) {
        this.cardPaystatus = cardPaystatus;
    }

    public String getFkBookingTime() {
        return fkBookingTime;
    }

    public void setFkBookingTime(String fkBookingTime) {
        this.fkBookingTime = fkBookingTime;
    }

    public String getFkBookingDate() {
        return fkBookingDate;
    }

    public void setFkBookingDate(String fkBookingDate) {
        this.fkBookingDate = fkBookingDate;
    }

    public Integer getExchCount() {
        return exchCount;
    }

    public void setExchCount(Integer exchCount) {
        this.exchCount = exchCount;
    }

    public String getSourceLastUpdatetime() {
        return sourceLastUpdatetime;
    }

    public void setSourceLastUpdatetime(String sourceLastUpdatetime) {
        this.sourceLastUpdatetime = sourceLastUpdatetime;
    }

    public String getSystemCreatetime() {
        return systemCreatetime;
    }

    public void setSystemCreatetime(String systemCreatetime) {
        this.systemCreatetime = systemCreatetime;
    }

    public String getSystemLastUpdatetime() {
        return systemLastUpdatetime;
    }

    public void setSystemLastUpdatetime(String systemLastUpdatetime) {
        this.systemLastUpdatetime = systemLastUpdatetime;
    }

    @Override
    public String toString() {
        return "ZsfMultiCardRedeemFactModel{" +
                "pkId='" + pkId + '\'' +
                ", akOrdernum='" + akOrdernum + '\'' +
                ", prodNo='" + prodNo + '\'' +
                ", depcity='" + depcity + '\'' +
                ", arrcity='" + arrcity + '\'' +
                ", cabin='" + cabin + '\'' +
                ", cnName='" + cnName + '\'' +
                ", certNo='" + certNo + '\'' +
                ", passengerUser='" + passengerUser + '\'' +
                ", psgrName='" + psgrName + '\'' +
                ", depDate=" + depDate +
                ", depTime='" + depTime + '\'' +
                ", cardNo='" + cardNo + '\'' +
                ", cardAmount=" + cardAmount +
                ", cardPaystatus='" + cardPaystatus + '\'' +
                ", fkBookingTime='" + fkBookingTime + '\'' +
                ", fkBookingDate='" + fkBookingDate + '\'' +
                ", exchCount=" + exchCount +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
