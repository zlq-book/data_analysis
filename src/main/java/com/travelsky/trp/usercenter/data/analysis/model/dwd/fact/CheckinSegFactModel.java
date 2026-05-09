package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;


public class CheckinSegFactModel {
  
  /**
   * 主键ID
   */
  @JsonProperty("PK_ID")
  private String pkId;
  /**
   * 票号
   */
  @JsonProperty("TIK_NUM")
  private String tikNum;
  /**
   * 值机日期
   */
  @JsonProperty("FK_CHECKIN_DATE")
  private String fkCheckinDate;
  /**
   * 值机时间
   */
  @JsonProperty("FK_CHECKIN_TIME")
  private String fkCheckinTime;
  /**
   * 乘机人类型（成人、儿童、无陪儿童等）
   */
  @JsonProperty("PASSENGER_TYPE")
  private String passengerType;
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
   * 乘机人证件类型
   */
  @JsonProperty("CERT_TYPE")
  private String certType;
  /**
   * 乘机人证件号码
   */
  @JsonProperty("CERT_NUMBER")
  private String certNumber;
  /**
   * 座位特征
   */
  @JsonProperty("SEAT_FEATURE")
  private String seatFeature;
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
   * 常客卡号
   */
  @JsonProperty("FFRF")
  private String ffrf;
  /**
   * 常客等级
   */
  @JsonProperty("FF_LEVEL")
  private String ffLevel;
  /**
   * 常客卡航司
   */
  @JsonProperty("FF_AIRLINE")
  private String ffAirline;
  /**
   * 常客联盟卡级别
   */
  @JsonProperty("FF_ALLIANCE_LEVEL")
  private String ffAllianceLevel;
  /**
   * 航程
   */
  @JsonProperty("FK_CHECKIN_SEG")
  private String fkCheckinSeg;
  /**
   * 航班起飞日期
   */
  @JsonProperty("FK_SEG_DATE")
  private String fkSegDate;
  /**
   * 航班起飞时间
   */
  @JsonProperty("FK_SEG_TIME")
  private String fkSegTime;
  /**
   * 起飞机场
   */
  @JsonProperty("FK_DEPAIRPORT")
  private String fkDepairport;
  /**
   * 到达机场
   */
  @JsonProperty("FK_ARRIAIRPORT")
  private String fkArriairport;
  /**
   * 值机终端
   */
  @JsonProperty("AK_CHECKIN_PID")
  private String akCheckinPid;
  /**
   * 值机Agent
   */
  @JsonProperty("CHECKIN_AGENT")
  private String checkinAgent;
  /**
   * 座位号
   */
  @JsonProperty("AK_SEAT_ASSIGN")
  private String akSeatAssign;
  /**
   * 国内国际标识
   */
  @JsonProperty("AK_DIMARK")
  private String akDimark;
  /**
   * 舱位
   */
  @JsonProperty("AK_SEGCABIN")
  private String akSegcabin;
  /**
   * 舱等
   */
  @JsonProperty("AK_CABIN")
  private String akCabin;
  /**
   * 值机人次
   */
  @JsonProperty("CHECKIN_COUNT")
  private Long checkinCount;
  /**
   * 值机状态
   */
  @JsonProperty("AK_CHECKIN_STATUS")
  private String akCheckinStatus;
  /**
   * 值机方式
   */
  @JsonProperty("AK_CHECKIN_TYPE")
  private String akCheckinType;
  /**
   * 是否携带行李
   */
  @JsonProperty("AK_BAGGAGE_FLAG")
  private Boolean akBaggageFlag;
  /**
   * 行李总件数
   */
  @JsonProperty("SEG_BAGGAGECOUNT")
  private Integer segBaggagecount;
  /**
   * 行李总重量
   */
  @JsonProperty("SEG_BAGGAGEWEIGHT")
  private Double segBaggageweight;
  /**
   * 网上值机渠道
   */
  @JsonProperty("CHECKIN_CHANNEL")
  private String checkinChannel;
  /**
   * 提前值机小时
   */
  @JsonProperty("EARLY_CHECKINTIME")
  private Long earlyCheckintime;

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

  public String getPkId() {
    return pkId;
  }

  public void setPkId(String pkId) {
    this.pkId = pkId;
  }

  public String getTikNum() {
    return tikNum;
  }

  public void setTikNum(String tikNum) {
    this.tikNum = tikNum;
  }

  public String getFkCheckinDate() {
    return fkCheckinDate;
  }

  public void setFkCheckinDate(String fkCheckinDate) {
    this.fkCheckinDate = fkCheckinDate;
  }

  public String getFkCheckinTime() {
    return fkCheckinTime;
  }

  public void setFkCheckinTime(String fkCheckinTime) {
    this.fkCheckinTime = fkCheckinTime;
  }

  public String getPassengerType() {
    return passengerType;
  }

  public void setPassengerType(String passengerType) {
    this.passengerType = passengerType;
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

  public String getFfrf() {
    return ffrf;
  }

  public void setFfrf(String ffrf) {
    this.ffrf = ffrf;
  }

  public String getFfLevel() {
    return ffLevel;
  }

  public void setFfLevel(String ffLevel) {
    this.ffLevel = ffLevel;
  }

  public String getFfAirline() {
    return ffAirline;
  }

  public void setFfAirline(String ffAirline) {
    this.ffAirline = ffAirline;
  }

  public String getFfAllianceLevel() {
    return ffAllianceLevel;
  }

  public void setFfAllianceLevel(String ffAllianceLevel) {
    this.ffAllianceLevel = ffAllianceLevel;
  }

  public String getFkCheckinSeg() {
    return fkCheckinSeg;
  }

  public void setFkCheckinSeg(String fkCheckinSeg) {
    this.fkCheckinSeg = fkCheckinSeg;
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

  public String getFkDepairport() {
    return fkDepairport;
  }

  public void setFkDepairport(String fkDepairport) {
    this.fkDepairport = fkDepairport;
  }

  public String getFkArriairport() {
    return fkArriairport;
  }

  public void setFkArriairport(String fkArriairport) {
    this.fkArriairport = fkArriairport;
  }

  public String getAkCheckinPid() {
    return akCheckinPid;
  }

  public void setAkCheckinPid(String akCheckinPid) {
    this.akCheckinPid = akCheckinPid;
  }

  public String getAkSeatAssign() {
    return akSeatAssign;
  }

  public void setAkSeatAssign(String akSeatAssign) {
    this.akSeatAssign = akSeatAssign;
  }

  public String getAkDimark() {
    return akDimark;
  }

  public void setAkDimark(String akDimark) {
    this.akDimark = akDimark;
  }

  public String getAkSegcabin() {
    return akSegcabin;
  }

  public void setAkSegcabin(String akSegcabin) {
    this.akSegcabin = akSegcabin;
  }

  public String getAkCabin() {
    return akCabin;
  }

  public void setAkCabin(String akCabin) {
    this.akCabin = akCabin;
  }

  public Long getCheckinCount() {
    return checkinCount;
  }

  public void setCheckinCount(Long checkinCount) {
    this.checkinCount = checkinCount;
  }

  public String getAkCheckinStatus() {
    return akCheckinStatus;
  }

  public void setAkCheckinStatus(String akCheckinStatus) {
    this.akCheckinStatus = akCheckinStatus;
  }

  public String getAkCheckinType() {
    return akCheckinType;
  }

  public void setAkCheckinType(String akCheckinType) {
    this.akCheckinType = akCheckinType;
  }

  public Boolean getAkBaggageFlag() {
    return akBaggageFlag;
  }

  public void setAkBaggageFlag(Boolean akBaggageFlag) {
    this.akBaggageFlag = akBaggageFlag;
  }

  public Integer getSegBaggagecount() {
    return segBaggagecount;
  }

  public void setSegBaggagecount(Integer segBaggagecount) {
    this.segBaggagecount = segBaggagecount;
  }

  public Double getSegBaggageweight() {
    return segBaggageweight;
  }

  public void setSegBaggageweight(Double segBaggageweight) {
    this.segBaggageweight = segBaggageweight;
  }

  public String getCheckinChannel() {
    return checkinChannel;
  }

  public void setCheckinChannel(String checkinChannel) {
    this.checkinChannel = checkinChannel;
  }

  public Long getEarlyCheckintime() {
    return earlyCheckintime;
  }

  public void setEarlyCheckintime(Long earlyCheckintime) {
    this.earlyCheckintime = earlyCheckintime;
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

  public String getCheckinAgent() {
    return checkinAgent;
  }

  public void setCheckinAgent(String checkinAgent) {
    this.checkinAgent = checkinAgent;
  }

  public String getSeatFeature() {
    return seatFeature;
  }

  public void setSeatFeature(String seatFeature) {
    this.seatFeature = seatFeature;
  }
}
