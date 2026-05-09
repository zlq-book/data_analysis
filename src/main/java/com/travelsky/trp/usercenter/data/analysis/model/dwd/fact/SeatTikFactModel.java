package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;


public class SeatTikFactModel {
  /**
   * 主键ID
   */
  @JsonProperty("PK_ID")
  private String pkId;
  /**
   * EMD出票日期
   */
  @JsonProperty("FK_SEATTIK_DATE")
  private String fkSeattikDate;
  /**
   * EMD出票时间
   */
  @JsonProperty("FK_SEATTIK_TIME")
  private String fkSeattikTime;
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
  @JsonProperty("FK_SEATSTART_DATE")
  private String fkSeatstartDate;
  /**
   * 航班时间
   */
  @JsonProperty("FK_SEATSTART_TIME")
  private String fkSeatstartTime;
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
  @JsonProperty("FK_SEAT_SEG")
  private String fkSeatSeg;
  /**
   * 国内国际标识
   */
  @JsonProperty("AK_DIMARK")
  private String akDimark;
  /**
   * 关联机票票号
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
   * 舱等
   */
  @JsonProperty("AK_CABIN")
  private String akCabin;
  /**
   * 舱位
   */
  @JsonProperty("AK_SEGCABIN")
  private String akSegcabin;
  /**
   * 原币种
   */
  @JsonProperty("AK_CURRENCY")
  private String akCurrency;
  /**
   * 原币种选座金额
   */
  @JsonProperty("SEAT_AMOUNT")
  private Double seatAmount;
  /**
   * 选座金额CNY
   */
  @JsonProperty("SEAT_AMOUNT_CNY")
  private Double seatAmountCny;
  /**
   * 选座详情
   */
  @JsonProperty("SEAT_DETAIL")
  private String seatDetail;
  /**
   * 座位区域代码
   */
  @JsonProperty("SEAT_ATT")
  private String seatAtt;
  /**
   * 座位号
   */
  @JsonProperty("SEAT_NUMBER")
  private String seatNumber;
  /**
   * 选座出票次数计数
   */
  @JsonProperty("SEAT_COUNT")
  private Integer seatCount = 1;
  /**
   * EMD出票OFFICE
   */
  @JsonProperty("AK_EMDTIKCHANNEL")
  private String akEmdtikchannel;
  /**
   * EMD出票OFFICE对应的IATACODE
   */
  @JsonProperty("AK_EMDIATACODE")
  private String akEmdiatacode;
  /**
   * EMD类型
   */
  @JsonProperty("AK_EMDTYPE")
  private String akEmdtype;
  /**
   * 关联订单号
   */
  @JsonProperty("AK_ORDERNUM")
  private String akOrdernum;
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
   * 预订渠道
   */
  @JsonProperty("AK_CHANNEL")
  private String akChannel;
  /**
   * 是否随票购买
   */
  @JsonProperty("PURCHASED_WITH_TICKET")
  private Boolean purchasedWithTicket;
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
   * 源系统最后更新时间
   */
  @JsonProperty("SOURCE_LAST_UPDATETIME")
  private String sourceLastUpdatetime;
  /**
   * 本系统创建日期时间
   */
  @JsonProperty("SYSTEM_CREATETIME")
  private String systemCreatetime;
  /**
   * 本系统最后更新日期时间
   */
  @JsonProperty("SYSTEM_LAST_UPDATETIME")
  private String systemLastUpdatetime = LocalDateTime.now().toString();


  public String getPkId() {
    return pkId;
  }

  public void setPkId(String pkId) {
    this.pkId = pkId;
  }

  public String getFkSeattikDate() {
    return fkSeattikDate;
  }

  public void setFkSeattikDate(String fkSeattikDate) {
    this.fkSeattikDate = fkSeattikDate;
  }

  public String getFkSeattikTime() {
    return fkSeattikTime;
  }

  public void setFkSeattikTime(String fkSeattikTime) {
    this.fkSeattikTime = fkSeattikTime;
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

  public String getFkSeatstartDate() {
    return fkSeatstartDate;
  }

  public void setFkSeatstartDate(String fkSeatstartDate) {
    this.fkSeatstartDate = fkSeatstartDate;
  }

  public String getFkSeatstartTime() {
    return fkSeatstartTime;
  }

  public void setFkSeatstartTime(String fkSeatstartTime) {
    this.fkSeatstartTime = fkSeatstartTime;
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

  public String getFkSeatSeg() {
    return fkSeatSeg;
  }

  public void setFkSeatSeg(String fkSeatSeg) {
    this.fkSeatSeg = fkSeatSeg;
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

  public String getAkCabin() {
    return akCabin;
  }

  public void setAkCabin(String akCabin) {
    this.akCabin = akCabin;
  }

  public String getAkSegcabin() {
    return akSegcabin;
  }

  public void setAkSegcabin(String akSegcabin) {
    this.akSegcabin = akSegcabin;
  }

  public String getAkCurrency() {
    return akCurrency;
  }

  public void setAkCurrency(String akCurrency) {
    this.akCurrency = akCurrency;
  }

  public Double getSeatAmount() {
    return seatAmount;
  }

  public void setSeatAmount(Double seatAmount) {
    this.seatAmount = seatAmount;
  }

  public String getSeatDetail() {
    return seatDetail;
  }

  public void setSeatDetail(String seatDetail) {
    this.seatDetail = seatDetail;
  }

  public String getSeatAtt() {
    return seatAtt;
  }

  public void setSeatAtt(String seatAtt) {
    this.seatAtt = seatAtt;
  }

  public String getSeatNumber() {
    return seatNumber;
  }

  public void setSeatNumber(String seatNumber) {
    this.seatNumber = seatNumber;
  }

  public Integer getSeatCount() {
    return seatCount;
  }

  public void setSeatCount(Integer seatCount) {
    this.seatCount = seatCount;
  }

  public String getAkEmdtikchannel() {
    return akEmdtikchannel;
  }

  public void setAkEmdtikchannel(String akEmdtikchannel) {
    this.akEmdtikchannel = akEmdtikchannel;
  }

  public String getAkEmdiatacode() {
    return akEmdiatacode;
  }

  public void setAkEmdiatacode(String akEmdiatacode) {
    this.akEmdiatacode = akEmdiatacode;
  }

  public String getAkEmdtype() {
    return akEmdtype;
  }

  public void setAkEmdtype(String akEmdtype) {
    this.akEmdtype = akEmdtype;
  }

  public String getAkOrdernum() {
    return akOrdernum;
  }

  public void setAkOrdernum(String akOrdernum) {
    this.akOrdernum = akOrdernum;
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

  public String getAkChannel() {
    return akChannel;
  }

  public void setAkChannel(String akChannel) {
    this.akChannel = akChannel;
  }

  public Boolean getPurchasedWithTicket() {
    return purchasedWithTicket;
  }

  public void setPurchasedWithTicket(Boolean purchasedWithTicket) {
    this.purchasedWithTicket = purchasedWithTicket;
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

  public Double getSeatAmountCny() {
    return seatAmountCny;
  }

  public void setSeatAmountCny(Double seatAmountCny) {
    this.seatAmountCny = seatAmountCny;
  }

  @Override
  public String toString() {
    return "SeatTikFactModel{" +
            "pkId='" + pkId + '\'' +
            ", fkSeattikDate='" + fkSeattikDate + '\'' +
            ", fkSeattikTime='" + fkSeattikTime + '\'' +
            ", depairport='" + depairport + '\'' +
            ", arriairport='" + arriairport + '\'' +
            ", fkSeatstartDate='" + fkSeatstartDate + '\'' +
            ", fkSeatstartTime='" + fkSeatstartTime + '\'' +
            ", enLastName='" + enLastName + '\'' +
            ", enFirstName='" + enFirstName + '\'' +
            ", cnName='" + cnName + '\'' +
            ", passengerType='" + passengerType + '\'' +
            ", certType='" + certType + '\'' +
            ", certNumber='" + certNumber + '\'' +
            ", passengerAge=" + passengerAge +
            ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
            ", fkSeatSeg='" + fkSeatSeg + '\'' +
            ", akDimark='" + akDimark + '\'' +
            ", akTiknum='" + akTiknum + '\'' +
            ", akEmdnum='" + akEmdnum + '\'' +
            ", akEmdStatus='" + akEmdStatus + '\'' +
            ", akCabin='" + akCabin + '\'' +
            ", akSegcabin='" + akSegcabin + '\'' +
            ", akCurrency='" + akCurrency + '\'' +
            ", seatAmount=" + seatAmount +
            ", seatAmountCny=" + seatAmountCny +
            ", seatDetail='" + seatDetail + '\'' +
            ", seatAtt='" + seatAtt + '\'' +
            ", seatNumber='" + seatNumber + '\'' +
            ", seatCount=" + seatCount +
            ", akEmdtikchannel='" + akEmdtikchannel + '\'' +
            ", akEmdiatacode='" + akEmdiatacode + '\'' +
            ", akEmdtype='" + akEmdtype + '\'' +
            ", akOrdernum='" + akOrdernum + '\'' +
            ", fkBookingUserTid='" + fkBookingUserTid + '\'' +
            ", fkBookingUserOriginId='" + fkBookingUserOriginId + '\'' +
            ", akChannel='" + akChannel + '\'' +
            ", purchasedWithTicket=" + purchasedWithTicket +
            ", dataActive=" + dataActive +
            ", dataActiveTime='" + dataActiveTime + '\'' +
            ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
            ", systemCreatetime='" + systemCreatetime + '\'' +
            ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
            '}';
  }
}
