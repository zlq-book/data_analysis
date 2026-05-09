package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * 附加服务升舱出票航段事实表实体类-T_DWD_UPGR_TIK_FACT
 * @author yuzc
 * @date 2025/7/30
 */
public class UpgrTikFactModel {
    /**
     * pk_id
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * EMD出票日期
     */
    @JsonProperty("FK_UPGRTIK_DATE")
    private String fkUpgrTikDate;

    /**
     * EMD出票时间
     */
    @JsonProperty("FK_UPGRTIK_TIME")
    private String fkUpgrTikTime;

    /**
     * 起飞机场
     */
    @JsonProperty("DEPAIRPORT")
    private String depairport;

    /**
     * 到达机场
     */
    @JsonProperty("ARRIAIRPORT")
    private String arriairport;

    /**
     * 航班日期
     */
    @JsonProperty("FK_SEG_DATE")
    private String fkSegDate;

    /**
     * 航班时间
     */
    @JsonProperty("FK_SEG_TIME")
    private String fkSegTime;

    /**
     * 乘机人英文姓
     */
    @JsonProperty("EN_LAST_NAME")
    private String enLastName;

    /**
     * 乘机人英文名
     */
    @JsonProperty("EN_FIRST_NAME")
    private String enFirstName;

    /**
     * 乘机人中文姓名
     */
    @JsonProperty("CN_NAME")
    private String cnName;

    /**
     * 乘机人类型
     */
    @JsonProperty("PASSENGER_TYPE")
    private String passengerType;

    /**
     * 乘机人证件类型
     */
    @JsonProperty("CERT_TYPE")
    private String certType;

    /**
     * 乘机人证件号
     */
    @JsonProperty("CERT_NUMBER")
    private String certNumber;

    /**
     * 乘机人年龄
     */
    @JsonProperty("PASSENGER_AGE")
    private Integer passengerAge;

    /**
     * 乘机人TID
     */
    @JsonProperty("FK_PASSENGER_USER_TID")
    private String fkPassengerUserTid;

    /**
     * 航程
     */
    @JsonProperty("FK_CABINUPGRINV_SEG")
    private String fkCabinupgrinvSeg;

    /**
     * 国内国际标识
     */
    @JsonProperty("AK_DIMARK")
    private String akDimark;

    /**
     * 关联票号
     */
    @JsonProperty("AK_TIKNUM")
    private String akTiknum;

    /**
     * EMD票号
     */
    @JsonProperty("AK_EMDNUM")
    private String akEmdnum;

    /**
     * EMD票状态
     */
    @JsonProperty("AK_EMD_STATUS")
    private String akEmdStatus;

    /**
     * 升舱前舱位
     */
    @JsonProperty("AK_UPGRDOLD_CABIN")
    private String akUpgrdoldCabin;

    /**
     * 升舱后舱位
     */
    @JsonProperty("AK_UPGRDNEW_CABIN")
    private String akUpgrdnewCabin;

    /**
     * 升舱前舱等
     */
    @JsonProperty("UPGRDOLD_CLASS")
    private String upgrdoldClass;

    /**
     * 升舱后舱等
     */
    @JsonProperty("UPGRDNEW_CLASS")
    private String upgrdnewClass;

    /**
     * 币种
     */
    @JsonProperty("AK_CURRENCY")
    private String akCurrency;

    /**
     * 升舱金额
     */
    @JsonProperty("UPGRDOLD_AMOUNT")
    private Double upgrdoldAmount;

    /**
     * 升舱详情
     */
    @JsonProperty("UPGRDOLD_DETTAIL")
    private String upgrdoldDettail;

    /**
     * EMD出票office
     */
    @JsonProperty("AK_EMDTIKCHANNEL")
    private String akEmdtikchannel;

    /**
     * EMD出票Office对应的IataCode
     */
    @JsonProperty("AK_EMDIATACODE")
    private String akEmdiataCode;

    /**
     * EMD类型
     */
    @JsonProperty("AK_EMDTYPE")
    private String akEmdtype;

    /**
     * 升舱服务出票数量
     */
    @JsonProperty("UPGR_TIK_COUNT")
    private Integer upgrTikCount = 1;

    /**
     * 源系统最后更新时间（时间戳）
     */
    @JsonProperty("SOURCE_LAST_UPDATETIME")
    private String sourceLastUpdatetime;

    /**
     * 数据是否启用
     */
    @JsonProperty("DATA_ACTIVE")
    private Boolean dataActive;

    /**
     * 数据启用时间
     */
    @JsonProperty("DATA_ACTIVE_TIME")
    private String dataActiveTime;

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

    /**
     * 关联订单号
     */
    @JsonProperty("AK_ORDERNUM")
    private String akOrdernum;

    /**
     * 支付方式
     */
    @JsonProperty("PAY_TYPE")
    private String payType;

    /**
     * 是否使用升舱券
     */
    @JsonProperty("IS_USE_UPGR_CUPON")
    private Boolean isUseUpgrCoupon;

    /**
     * 预订渠道
     */
    @JsonProperty("AK_BOOK_CHANNEL")
    private String akBookChannel;

    /**
     * 预订人TID
     */
    @JsonProperty("FK_BOOKING_USER_TID")
    private String fkBookingUserTid;

    /**
     * 升舱金额CNY
     */
    @JsonProperty("UPGRDOLD_AMOUNT_CNY")
    private Double upgrdoldAmountCny;

    // Getter and Setter methods

    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getFkUpgrTikDate() {
        return fkUpgrTikDate;
    }

    public void setFkUpgrTikDate(String fkUpgrTikDate) {
        this.fkUpgrTikDate = fkUpgrTikDate;
    }

    public String getFkUpgrTikTime() {
        return fkUpgrTikTime;
    }

    public void setFkUpgrTikTime(String fkUpgrTikTime) {
        this.fkUpgrTikTime = fkUpgrTikTime;
    }

    public String getDepairport() {
        return depairport;
    }

    public void setDepairport(String depairport) {
        this.depairport = depairport;
    }

    public String getArriairport() {
        return arriairport;
    }

    public void setArriairport(String arriairport) {
        this.arriairport = arriairport;
    }

    public String getFkSegDate() {
        return fkSegDate;
    }

    public void setFkSegDate(String fkSegDate) {
        this.fkSegDate = fkSegDate;
    }

    public String getFkSegTime() {
        return fkSegTime;
    }

    public void setFkSegTime(String fkSegTime) {
        this.fkSegTime = fkSegTime;
    }

    public String getEnLastName() {
        return enLastName;
    }

    public void setEnLastName(String enLastName) {
        this.enLastName = enLastName;
    }

    public String getEnFirstName() {
        return enFirstName;
    }

    public void setEnFirstName(String enFirstName) {
        this.enFirstName = enFirstName;
    }

    public String getCnName() {
        return cnName;
    }

    public void setCnName(String cnName) {
        this.cnName = cnName;
    }

    public String getPassengerType() {
        return passengerType;
    }

    public void setPassengerType(String passengerType) {
        this.passengerType = passengerType;
    }

    public String getCertType() {
        return certType;
    }

    public void setCertType(String certType) {
        this.certType = certType;
    }

    public String getCertNumber() {
        return certNumber;
    }

    public void setCertNumber(String certNumber) {
        this.certNumber = certNumber;
    }

    public Integer getPassengerAge() {
        return passengerAge;
    }

    public void setPassengerAge(Integer passengerAge) {
        this.passengerAge = passengerAge;
    }

    public String getFkPassengerUserTid() {
        return fkPassengerUserTid;
    }

    public void setFkPassengerUserTid(String fkPassengerUserTid) {
        this.fkPassengerUserTid = fkPassengerUserTid;
    }

    public String getFkCabinupgrinvSeg() {
        return fkCabinupgrinvSeg;
    }

    public void setFkCabinupgrinvSeg(String fkCabinupgrinvSeg) {
        this.fkCabinupgrinvSeg = fkCabinupgrinvSeg;
    }

    public String getAkDimark() {
        return akDimark;
    }

    public void setAkDimark(String akDimark) {
        this.akDimark = akDimark;
    }

    public String getAkTiknum() {
        return akTiknum;
    }

    public void setAkTiknum(String akTiknum) {
        this.akTiknum = akTiknum;
    }

    public String getAkEmdnum() {
        return akEmdnum;
    }

    public void setAkEmdnum(String akEmdnum) {
        this.akEmdnum = akEmdnum;
    }

    public String getAkEmdStatus() {
        return akEmdStatus;
    }

    public void setAkEmdStatus(String akEmdStatus) {
        this.akEmdStatus = akEmdStatus;
    }

    public String getAkUpgrdoldCabin() {
        return akUpgrdoldCabin;
    }

    public void setAkUpgrdoldCabin(String akUpgrdoldCabin) {
        this.akUpgrdoldCabin = akUpgrdoldCabin;
    }

    public String getAkUpgrdnewCabin() {
        return akUpgrdnewCabin;
    }

    public void setAkUpgrdnewCabin(String akUpgrdnewCabin) {
        this.akUpgrdnewCabin = akUpgrdnewCabin;
    }

    public String getUpgrdoldClass() {
        return upgrdoldClass;
    }

    public void setUpgrdoldClass(String upgrdoldClass) {
        this.upgrdoldClass = upgrdoldClass;
    }

    public String getUpgrdnewClass() {
        return upgrdnewClass;
    }

    public void setUpgrdnewClass(String upgrdnewClass) {
        this.upgrdnewClass = upgrdnewClass;
    }

    public String getAkCurrency() {
        return akCurrency;
    }

    public void setAkCurrency(String akCurrency) {
        this.akCurrency = akCurrency;
    }

    public Double getUpgrdoldAmount() {
        return upgrdoldAmount;
    }

    public void setUpgrdoldAmount(Double upgrdoldAmount) {
        this.upgrdoldAmount = upgrdoldAmount;
    }

    public String getUpgrdoldDettail() {
        return upgrdoldDettail;
    }

    public void setUpgrdoldDettail(String upgrdoldDettail) {
        this.upgrdoldDettail = upgrdoldDettail;
    }

    public String getAkEmdtikchannel() {
        return akEmdtikchannel;
    }

    public void setAkEmdtikchannel(String akEmdtikchannel) {
        this.akEmdtikchannel = akEmdtikchannel;
    }

    public String getAkEmdiataCode() {
        return akEmdiataCode;
    }

    public void setAkEmdiataCode(String akEmdiataCode) {
        this.akEmdiataCode = akEmdiataCode;
    }

    public String getAkEmdtype() {
        return akEmdtype;
    }

    public void setAkEmdtype(String akEmdtype) {
        this.akEmdtype = akEmdtype;
    }

    public Integer getUpgrTikCount() {
        return upgrTikCount;
    }

    public void setUpgrTikCount(Integer upgrTikCount) {
        this.upgrTikCount = upgrTikCount;
    }

    public String getSourceLastUpdatetime() {
        return sourceLastUpdatetime;
    }

    public void setSourceLastUpdatetime(String sourceLastUpdatetime) {
        this.sourceLastUpdatetime = sourceLastUpdatetime;
    }

    public Boolean getDataActive() {
        return dataActive;
    }

    public void setDataActive(Boolean dataActive) {
        this.dataActive = dataActive;
    }

    public String getDataActiveTime() {
        return dataActiveTime;
    }

    public void setDataActiveTime(String dataActiveTime) {
        this.dataActiveTime = dataActiveTime;
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

    // -------------------------- 新增缺失字段的Getter & Setter --------------------------
    public String getAkOrdernum() {
        return akOrdernum;
    }

    public void setAkOrdernum(String akOrdernum) {
        this.akOrdernum = akOrdernum;
    }

    public String getPayType() {
        return payType;
    }

    public void setPayType(String payType) {
        this.payType = payType;
    }

    public Boolean getUseUpgrCoupon() {
        return isUseUpgrCoupon;
    }

    public void setUseUpgrCoupon(Boolean useUpgrCoupon) {
        isUseUpgrCoupon = useUpgrCoupon;
    }

    public String getAkBookChannel() {
        return akBookChannel;
    }

    public void setAkBookChannel(String akBookChannel) {
        this.akBookChannel = akBookChannel;
    }

    public String getFkBookingUserTid() {
        return fkBookingUserTid;
    }

    public void setFkBookingUserTid(String fkBookingUserTid) {
        this.fkBookingUserTid = fkBookingUserTid;
    }

    public Double getUpgrdoldAmountCny() {
        return upgrdoldAmountCny;
    }

    public void setUpgrdoldAmountCny(Double upgrdoldAmountCny) {
        this.upgrdoldAmountCny = upgrdoldAmountCny;
    }


    @Override
    public String toString() {
        return "UpgrTikFactModel{" +
                "pkId='" + pkId + '\'' +
                ", fkUpgrTikDate='" + fkUpgrTikDate + '\'' +
                ", fkUpgrTikTime='" + fkUpgrTikTime + '\'' +
                ", depairport='" + depairport + '\'' +
                ", arriairport='" + arriairport + '\'' +
                ", fkSegDate='" + fkSegDate + '\'' +
                ", fkSegTime='" + fkSegTime + '\'' +
                ", enLastName='" + enLastName + '\'' +
                ", enFirstName='" + enFirstName + '\'' +
                ", cnName='" + cnName + '\'' +
                ", passengerType='" + passengerType + '\'' +
                ", certType='" + certType + '\'' +
                ", certNumber='" + certNumber + '\'' +
                ", passengerAge=" + passengerAge +
                ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
                ", fkCabinupgrinvSeg='" + fkCabinupgrinvSeg + '\'' +
                ", akDimark='" + akDimark + '\'' +
                ", akTiknum='" + akTiknum + '\'' +
                ", akEmdnum='" + akEmdnum + '\'' +
                ", akEmdStatus='" + akEmdStatus + '\'' +
                ", akUpgrdoldCabin='" + akUpgrdoldCabin + '\'' +
                ", akUpgrdnewCabin='" + akUpgrdnewCabin + '\'' +
                ", upgrdoldClass='" + upgrdoldClass + '\'' +
                ", upgrdnewClass='" + upgrdnewClass + '\'' +
                ", akCurrency='" + akCurrency + '\'' +
                ", upgrdoldAmount=" + upgrdoldAmount +
                ", upgrdoldDettail='" + upgrdoldDettail + '\'' +
                ", akEmdtikchannel='" + akEmdtikchannel + '\'' +
                ", akEmdiataCode='" + akEmdiataCode + '\'' +
                ", akEmdtype='" + akEmdtype + '\'' +
                ", upgrTikCount=" + upgrTikCount +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", dataActive=" + dataActive +
                ", dataActiveTime='" + dataActiveTime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                ", akOrdernum='" + akOrdernum + '\'' +
                ", payType='" + payType + '\'' +
                ", isUseUpgrCoupon=" + isUseUpgrCoupon +
                ", akBookChannel='" + akBookChannel + '\'' +
                ", fkBookingUserTid='" + fkBookingUserTid + '\'' +
                ", upgrdoldAmountCny=" + upgrdoldAmountCny +
                '}';
    }
}