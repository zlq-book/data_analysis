package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 明细事实表-掌尚飞-次卡退订
 */
public class ZsfMultiCardCancelFactModel {

    /**
     * pk_id
     */
    @JsonProperty("PK_ID")
    private String pkId;

    public String getAkMainOrdernum() {
        return akMainOrdernum;
    }

    public void setAkMainOrdernum(String akMainOrdernum) {
        this.akMainOrdernum = akMainOrdernum;
    }

    /**
     * 子订单号
     */
    @JsonProperty("AK_ORDERNUM")
    private String akOrdernum;

    /**
     * 关联主订单号
     */
    @JsonProperty("ARK_MAINORDERNUM")
    private String akMainOrdernum;

    /**
     * 产品编号
     */
    @JsonProperty("PROD_NO")
    private String prodNo;

    /**
     * 套票次卡价格
     */
    @JsonProperty("SALE_PRICE")
    private BigDecimal salePrice;

    /**
     * 产品状态
     */
    @JsonProperty("PROD_STATE")
    private String prodState;

    /**
     * 受益人TID
     */
    @JsonProperty("BENEFI_TID")
    private String benefiTid;

    /**
     * 受益人证件类型
     */
    @JsonProperty("BENEFI_CERT_TYPE")
    private String benefiCertType;

    /**
     * 受益人证件号
     */
    @JsonProperty("BENEFI_CERT_NO")
    private String benefiCertNo;

    /**
     * 受益人中文姓
     */
    @JsonProperty("CN_LAST_NAME")
    private String cnLastName;

    /**
     * 受益人中文名
     */
    @JsonProperty("CN_FIRST_NAME")
    private String cnFirstName;

    /**
     * 受益人英文名
     */
    @JsonProperty("EN_FIRST_NAME")
    private String enFirstName;

    /**
     * 受益人英文姓
     */
    @JsonProperty("EN_LAST_NAME")
    private String enLastName;

    /**
     * 受益人证件有效期
     */
    @JsonProperty("BENEFI_EXPIRATION_DATE")
    private LocalDate benefiExpirationDate;

    /**
     * 受益人性别
     */
    @JsonProperty("BENEFI_SEX")
    private String benefiSex;

    /**
     * 受益人出生日期
     */
    @JsonProperty("BENEFI_BIRTHDAY")
    private LocalDate benefiBirthday;

    /**
     * 受益人护照签发国
     */
    @JsonProperty("BENEFI_PASSPORT_ISSUE_NATION")
    private String benefiPassportIssueNation;

    /**
     * 受益人护照国籍
     */
    @JsonProperty("BENEFI_PASSPORT_NATION")
    private String benefiPassportNation;

    /**
     * 受益人电话
     */
    @JsonProperty("BENEFI_MOBILE_NUMBER")
    private String benefiMobileNumber;

    /**
     * 激活时间
     */
    @JsonProperty("ACT_TIME")
    private String actTime;

    /**
     * 国际标识
     */
    @JsonProperty("INTER_FLAG")
    private String interFlag;

    /**
     * 兑换次数
     */
    @JsonProperty("EXCH_COUNT")
    private Integer exchCount;

    /**
     * 失效日期
     */
    @JsonProperty("EXPIR_DATE")
    private LocalDate expirDate;

    /**
     * 旅行开始日期
     */
    @JsonProperty("DEP_DATE")
    private LocalDate depDate;

    /**
     * 旅行结束日期
     */
    @JsonProperty("ARR_DATE")
    private LocalDate arrDate;

    /**
     * 预订渠道
     */
    @JsonProperty("BOOKING_CHANNEL")
    private String bookingChannel;

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
     * 预订人TID
     */
    @JsonProperty("FK_BOOKING_USER_TID")
    private String fkBookingUserTid;

    /**
     * 预订人源ID
     */
    @JsonProperty("FK_BOOKING_USER_ORIGIN_ID")
    private String fkBookingUserOriginId;

    /**
     * 退订次数计数
     */
    @JsonProperty("UNSUBSCRIBE_COUNT")
    private Integer unsubscribeCount = 1;

    /**
     * 源系统最后更新时间（时间戳）
     */
    @JsonProperty("SOURCE_LAST_UPDATETIME")
    private String sourceLastUpdatetime  = LocalDateTime.now().toString();

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

    public BigDecimal getSalePrice() {
        return salePrice;
    }

    public void setSalePrice(BigDecimal salePrice) {
        this.salePrice = salePrice;
    }

    public String getProdState() {
        return prodState;
    }

    public void setProdState(String prodState) {
        this.prodState = prodState;
    }

    public String getBenefiTid() {
        return benefiTid;
    }

    public void setBenefiTid(String benefiTid) {
        this.benefiTid = benefiTid;
    }

    public String getBenefiCertType() {
        return benefiCertType;
    }

    public void setBenefiCertType(String benefiCertType) {
        this.benefiCertType = benefiCertType;
    }

    public String getBenefiCertNo() {
        return benefiCertNo;
    }

    public void setBenefiCertNo(String benefiCertNo) {
        this.benefiCertNo = benefiCertNo;
    }

    public String getCnLastName() {
        return cnLastName;
    }

    public void setCnLastName(String cnLastName) {
        this.cnLastName = cnLastName;
    }

    public String getCnFirstName() {
        return cnFirstName;
    }

    public void setCnFirstName(String cnFirstName) {
        this.cnFirstName = cnFirstName;
    }

    public String getEnFirstName() {
        return enFirstName;
    }

    public void setEnFirstName(String enFirstName) {
        this.enFirstName = enFirstName;
    }

    public String getEnLastName() {
        return enLastName;
    }

    public void setEnLastName(String enLastName) {
        this.enLastName = enLastName;
    }

    public LocalDate getBenefiExpirationDate() {
        return benefiExpirationDate;
    }

    public void setBenefiExpirationDate(LocalDate benefiExpirationDate) {
        this.benefiExpirationDate = benefiExpirationDate;
    }

    public String getBenefiSex() {
        return benefiSex;
    }

    public void setBenefiSex(String benefiSex) {
        this.benefiSex = benefiSex;
    }

    public LocalDate getBenefiBirthday() {
        return benefiBirthday;
    }

    public void setBenefiBirthday(LocalDate benefiBirthday) {
        this.benefiBirthday = benefiBirthday;
    }

    public String getBenefiPassportIssueNation() {
        return benefiPassportIssueNation;
    }

    public void setBenefiPassportIssueNation(String benefiPassportIssueNation) {
        this.benefiPassportIssueNation = benefiPassportIssueNation;
    }

    public String getBenefiPassportNation() {
        return benefiPassportNation;
    }

    public void setBenefiPassportNation(String benefiPassportNation) {
        this.benefiPassportNation = benefiPassportNation;
    }

    public String getBenefiMobileNumber() {
        return benefiMobileNumber;
    }

    public void setBenefiMobileNumber(String benefiMobileNumber) {
        this.benefiMobileNumber = benefiMobileNumber;
    }

    public String getActTime() {
        return actTime;
    }

    public void setActTime(String actTime) {
        this.actTime = actTime;
    }

    public String getInterFlag() {
        return interFlag;
    }

    public void setInterFlag(String interFlag) {
        this.interFlag = interFlag;
    }

    public Integer getExchCount() {
        return exchCount;
    }

    public void setExchCount(Integer exchCount) {
        this.exchCount = exchCount;
    }

    public LocalDate getExpirDate() {
        return expirDate;
    }

    public void setExpirDate(LocalDate expirDate) {
        this.expirDate = expirDate;
    }

    public LocalDate getDepDate() {
        return depDate;
    }

    public void setDepDate(LocalDate depDate) {
        this.depDate = depDate;
    }

    public LocalDate getArrDate() {
        return arrDate;
    }

    public void setArrDate(LocalDate arrDate) {
        this.arrDate = arrDate;
    }

    public String getBookingChannel() {
        return bookingChannel;
    }

    public void setBookingChannel(String bookingChannel) {
        this.bookingChannel = bookingChannel;
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

    public String getFkBookingUserTid() {
        return fkBookingUserTid;
    }

    public void setFkBookingUserTid(String fkBookingUserTid) {
        this.fkBookingUserTid = fkBookingUserTid;
    }

    public String getFkBookingUserOriginId() {
        return fkBookingUserOriginId;
    }

    public void setFkBookingUserOriginId(String fkBookingUserOriginId) {
        this.fkBookingUserOriginId = fkBookingUserOriginId;
    }

    public Integer getUnsubscribeCount() {
        return unsubscribeCount;
    }

    public void setUnsubscribeCount(Integer unsubscribeCount) {
        this.unsubscribeCount = unsubscribeCount;
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
        return "ZsfMultiCardCancelFactModel{" +
                "pkId='" + pkId + '\'' +
                ", akOrdernum='" + akOrdernum + '\'' +
                ", prodNo='" + prodNo + '\'' +
                ", salePrice=" + salePrice +
                ", prodState='" + prodState + '\'' +
                ", benefiTid='" + benefiTid + '\'' +
                ", benefiCertType='" + benefiCertType + '\'' +
                ", benefiCertNo='" + benefiCertNo + '\'' +
                ", cnLastName='" + cnLastName + '\'' +
                ", cnFirstName='" + cnFirstName + '\'' +
                ", enFirstName='" + enFirstName + '\'' +
                ", enLastName='" + enLastName + '\'' +
                ", benefiExpirationDate=" + benefiExpirationDate +
                ", benefiSex='" + benefiSex + '\'' +
                ", benefiBirthday=" + benefiBirthday +
                ", benefiPassportIssueNation='" + benefiPassportIssueNation + '\'' +
                ", benefiPassportNation='" + benefiPassportNation + '\'' +
                ", benefiMobileNumber='" + benefiMobileNumber + '\'' +
                ", actTime='" + actTime + '\'' +
                ", interFlag='" + interFlag + '\'' +
                ", exchCount=" + exchCount +
                ", expirDate=" + expirDate +
                ", depDate=" + depDate +
                ", arrDate=" + arrDate +
                ", bookingChannel='" + bookingChannel + '\'' +
                ", fkBookingTime='" + fkBookingTime + '\'' +
                ", fkBookingDate='" + fkBookingDate + '\'' +
                ", fkBookingUserTid='" + fkBookingUserTid + '\'' +
                ", fkBookingUserOriginId='" + fkBookingUserOriginId + '\'' +
                ", unsubscribeCount=" + unsubscribeCount +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
