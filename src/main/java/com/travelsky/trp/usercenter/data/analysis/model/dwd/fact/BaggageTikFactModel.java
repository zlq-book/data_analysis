package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

public class BaggageTikFactModel {
  /**
   * 主键ID
   */
  @JsonProperty("PK_ID")
  private String pkId;
  /**
   * EMD出票日期
   */
  @JsonProperty("FK_BAGGAGETIK_DATE")
  private String fkBaggagetikDate;
  /**
   * EMD出票时间
   */
  @JsonProperty("FK_BAGGAGETIK_TIME")
  private String fkBaggagetikTime;
  /**
   *    起飞机场
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
  @JsonProperty("FK_BAGGAGESTART_DATE")
  private String fkBaggagestartDate;
  /**
   * 航班时间
   */
  @JsonProperty("FK_BAGGAGESTART_TIME")
  private String fkBaggagestartTime;
  /**
   * 乘机人英文姓
   */
  @JsonProperty("EN_LAST_NAME")
  private String enLastName;
  /**
   * 乘机人英文姓
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
  @JsonProperty("FK_BAGGAGE_SEG")
  private String fkBaggageSeg;
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
   * 原币种预付费行李金额
   */
  @JsonProperty("BAGGAGE_AMOUNT")
  private Double baggageAmount;
  /**
   * 预付费行李金额CNY
   */
  @JsonProperty("BAGGAGE_AMOUNT_CNY")
  private Double baggageAmountCny;
  /**
   * 行李详情
   */
  @JsonProperty("BAGGAGE_DETAIL")
  private String baggageDetail;
  /**
   * 预付费行李属性
   */
  @JsonProperty("AK_BAGGAGE_ATT")
  private String akBaggageAtt;
  /**
   * 预付费行李件数
   */
  @JsonProperty("BAGGAGE_BAGS")
  private Integer baggageBags;
  /**
   * 预付费行李重量
   */
  @JsonProperty("BAGGAGE_QUANTITY")
  private Integer baggageQuantity;
  /**
   * 预付费行李服务使用计数
   */
  @JsonProperty("BAGGAGE_COUNT")
  private Integer baggageCount = 1;
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

  public String getFkBaggagetikDate() {
    return fkBaggagetikDate;
  }

  public void setFkBaggagetikDate(String fkBaggagetikDate) {
    this.fkBaggagetikDate = fkBaggagetikDate;
  }

  public String getFkBaggagetikTime() {
    return fkBaggagetikTime;
  }

  public void setFkBaggagetikTime(String fkBaggagetikTime) {
    this.fkBaggagetikTime = fkBaggagetikTime;
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

  public String getFkBaggagestartDate() {
    return fkBaggagestartDate;
  }

  public void setFkBaggagestartDate(String fkBaggagestartDate) {
    this.fkBaggagestartDate = fkBaggagestartDate;
  }

  public String getFkBaggagestartTime() {
    return fkBaggagestartTime;
  }

  public void setFkBaggagestartTime(String fkBaggagestartTime) {
    this.fkBaggagestartTime = fkBaggagestartTime;
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

  public String getFkBaggageSeg() {
    return fkBaggageSeg;
  }

  public void setFkBaggageSeg(String fkBaggageSeg) {
    this.fkBaggageSeg = fkBaggageSeg;
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

  public Double getBaggageAmount() {
    return baggageAmount;
  }

  public void setBaggageAmount(Double baggageAmount) {
    this.baggageAmount = baggageAmount;
  }

  public String getBaggageDetail() {
    return baggageDetail;
  }

  public void setBaggageDetail(String baggageDetail) {
    this.baggageDetail = baggageDetail;
  }

  public String getAkBaggageAtt() {
    return akBaggageAtt;
  }

  public void setAkBaggageAtt(String akBaggageAtt) {
    this.akBaggageAtt = akBaggageAtt;
  }

  public Integer getBaggageCount() {
    return baggageCount;
  }

  public void setBaggageCount(Integer baggageCount) {
    this.baggageCount = baggageCount;
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

  public Integer getBaggageQuantity() {
    return baggageQuantity;
  }

  public void setBaggageQuantity(Integer baggageQuantity) {
    this.baggageQuantity = baggageQuantity;
  }

  public Integer getBaggageBags() {
    return baggageBags;
  }

  public void setBaggageBags(Integer baggageBags) {
    this.baggageBags = baggageBags;
  }

  public Double getBaggageAmountCny() {
    return baggageAmountCny;
  }

  public void setBaggageAmountCny(Double baggageAmountCny) {
    this.baggageAmountCny = baggageAmountCny;
  }
}
