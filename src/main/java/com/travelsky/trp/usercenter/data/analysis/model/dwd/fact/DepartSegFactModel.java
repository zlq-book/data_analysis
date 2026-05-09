package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;


public class DepartSegFactModel {
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
   * 航班起飞日期
   */
  @JsonProperty("FK_DEPARTURES_DATE")
  private String fkDeparturesDate;
  /**
   * 航班到达日期
   */
  @JsonProperty("FK_ARRIVE_DATE")
  private String fkArriveDate;
  /**
   * 航班起飞时间
   */
  @JsonProperty("FK_DEPARTURES_TIME")
  private String fkDeparturesTime;
  /**
   * 航班到达时间
   */
  @JsonProperty("FK_ARRIVE_TIME")
  private String fkArriveTime;
  /**
   * 航程
   */
  @JsonProperty("FK_PNRD_SEG")
  private String fkPnrdSeg;
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
   * 乘机人类型
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
   * 修正后乘机人证件类型
   */
  @JsonProperty("REVISED_CERT_TYPE")
  private String revisedCertType;
  /**
   * 乘机人证件号码
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
   * 离港舱位
   */
  @JsonProperty("AK_SEGCABIN")
  private String akSegcabin;
  /**
   * 离港舱等
   */
  @JsonProperty("AK_CABIN")
  private String akCabin;
  /**
   * 国内国际标识
   */
  @JsonProperty("AK_DIMARK")
  private String akDimark;
  /**
   * 出票日期
   */
  @JsonProperty("FK_ISSUE_DATE")
  private String fkIssueDate;

  /**
   * 出票时间
   */
  @JsonProperty("FK_ISSUE_TIME")
  private String fkIssueTime;
  /**
   * 常客卡号
   */
  @JsonProperty("FFRF")
  private String ffrf;
  /**
   * 飞行时是否使用常客卡
   * */
  @JsonProperty("IS_USED_EFR")
  private Boolean isUsedFfr;

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
   * 运输币种
   */
  @JsonProperty("TRANSPORT_CURRENCY")
  private String transportCurrency;
  /**
   * 航段运输票面价格CNY
   */
  @JsonProperty("SEG_TRANSPORT_TIK_PRICE")
  private Double segTransportTikPrice;
  /**
   * 航段运输代理费CNY
   */
  @JsonProperty("SEG_TRANSPORT_AGENCY_FEE")
  private Double segTransportAgencyFee;
  /**
   * 航段运输票面净额CNY
   */
  @JsonProperty("SEG_TRANSPORT_NET_AMOUNT")
  private Double segTransportNetAmount;
  /**
   * 代理人名称
   */
  @JsonProperty("AGENT")
  private String agent;
  /**
   * 停留地
   */
  @JsonProperty("STOPOVER")
  private String stopover;
  /**
   * 停留天数
   */
  @JsonProperty("STOPOVER_DAY")
  private Integer stopoverDay;
  /**
   * 提前出票天数
   */
  @JsonProperty("AK_ADVBOOK_DAY")
  private Integer akAdvbookDay;
  /**
   * 是否与儿童同行
   */
  @JsonProperty("PEER_CHD")
  private Boolean peerChd;
  /**
   * 是否与老人同行
   */
  @JsonProperty("PEER_SENIOR")
  private Boolean peerSenior;
  /**
   * 是否与婴儿同行
   */
  //@JsonProperty("PEER_INFANT")
  //private Boolean peerInfant;
  /**
   * 是否为自己订票
   */
  @JsonProperty("SELF_BOOKING")
  private Boolean selfBooking;
  /**
   * 是否首次乘机
   */
  @JsonProperty("FIRST_TRAVELER")
  private Boolean firstTraveler;
  /**
   * 是否是团队票
   */
  @JsonProperty("AK_TEAMMARK")
  private Boolean akTeammark;
  /**
   * 是否为返乡段
   */
  @JsonProperty("HOMECOMING_SEG")
  private Boolean homecomingSeg;
  /**
   * 成行航段计数
   */
  @JsonProperty("SEG_COUNT")
  private Integer segCount;
  /**
   * 是否为大客户订单
   */
  @JsonProperty("IS_KEY_ACCOUNT")
  private Boolean isKeyAccount;
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


  public String getTikNum() {
    return tikNum;
  }

  public void setTikNum(String tikNum) {
    this.tikNum = tikNum;
  }

  public String getFkDeparturesDate() {
    return fkDeparturesDate;
  }

  public void setFkDeparturesDate(String fkDeparturesDate) {
    this.fkDeparturesDate = fkDeparturesDate;
  }

  public String getFkArriveDate() {
    return fkArriveDate;
  }

  public void setFkArriveDate(String fkArriveDate) {
    this.fkArriveDate = fkArriveDate;
  }

  public String getFkDeparturesTime() {
    return fkDeparturesTime;
  }

  public void setFkDeparturesTime(String fkDeparturesTime) {
    this.fkDeparturesTime = fkDeparturesTime;
  }

  public String getFkArriveTime() {
    return fkArriveTime;
  }

  public void setFkArriveTime(String fkArriveTime) {
    this.fkArriveTime = fkArriveTime;
  }

  public String getFkPnrdSeg() {
    return fkPnrdSeg;
  }

  public void setFkPnrdSeg(String fkPnrdSeg) {
    this.fkPnrdSeg = fkPnrdSeg;
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


  public String getAkDimark() {
    return akDimark;
  }

  public void setAkDimark(String akDimark) {
    this.akDimark = akDimark;
  }

  public Boolean getFirstTraveler() {
    return firstTraveler;
  }

  public void setFirstTraveler(Boolean firstTraveler) {
    this.firstTraveler = firstTraveler;
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

  public Integer getSegCount() {
    return segCount;
  }

  public void setSegCount(Integer segCount) {
    this.segCount = segCount;
  }

  public Boolean getAkTeammark() {
    return akTeammark;
  }

  public void setAkTeammark(Boolean akTeammark) {
    this.akTeammark = akTeammark;
  }

  public Boolean getKeyAccount() {
    return isKeyAccount;
  }

  public void setKeyAccount(Boolean keyAccount) {
    isKeyAccount = keyAccount;
  }

  public Integer getAkAdvbookDay() {
    return akAdvbookDay;
  }

  public void setAkAdvbookDay(Integer akAdvbookDay) {
    this.akAdvbookDay = akAdvbookDay;
  }

  public Boolean getPeerChd() {
    return peerChd;
  }

  public void setPeerChd(Boolean peerChd) {
    this.peerChd = peerChd;
  }

  public Boolean getPeerSenior() {
    return peerSenior;
  }

  public void setPeerSenior(Boolean peerSenior) {
    this.peerSenior = peerSenior;
  }

  //public Boolean getPeerInfant() {
  //  return peerInfant;
  //}
  //
  //public void setPeerInfant(Boolean peerInfant) {
  //  this.peerInfant = peerInfant;
  //}

  public Boolean getSelfBooking() {
    return selfBooking;
  }

  public void setSelfBooking(Boolean selfBooking) {
    this.selfBooking = selfBooking;
  }

  public String getRevisedCertType() {
    return revisedCertType;
  }

  public void setRevisedCertType(String revisedCertType) {
    this.revisedCertType = revisedCertType;
  }

  public String getFkIssueDate() {
    return fkIssueDate;
  }

  public void setFkIssueDate(String fkIssueDate) {
    this.fkIssueDate = fkIssueDate;
  }

  public String getFkIssueTime() {
    return fkIssueTime;
  }

  public void setFkIssueTime(String fkIssueTime) {
    this.fkIssueTime = fkIssueTime;
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

  public String getTransportCurrency() {
    return transportCurrency;
  }

  public void setTransportCurrency(String transportCurrency) {
    this.transportCurrency = transportCurrency;
  }

  public Double getSegTransportTikPrice() {
    return segTransportTikPrice;
  }

  public void setSegTransportTikPrice(Double segTransportTikPrice) {
    this.segTransportTikPrice = segTransportTikPrice;
  }

  public Double getSegTransportAgencyFee() {
    return segTransportAgencyFee;
  }

  public void setSegTransportAgencyFee(Double segTransportAgencyFee) {
    this.segTransportAgencyFee = segTransportAgencyFee;
  }

  public Double getSegTransportNetAmount() {
    return segTransportNetAmount;
  }

  public void setSegTransportNetAmount(Double segTransportNetAmount) {
    this.segTransportNetAmount = segTransportNetAmount;
  }

  public String getStopover() {
    return stopover;
  }

  public void setStopover(String stopover) {
    this.stopover = stopover;
  }

  public Integer getStopoverDay() {
    return stopoverDay;
  }

  public void setStopoverDay(Integer stopoverDay) {
    this.stopoverDay = stopoverDay;
  }

  public Boolean getUsedFfr() {
    return isUsedFfr;
  }

  public void setUsedFfr(Boolean usedFfr) {
    isUsedFfr = usedFfr;
  }

  public String getAgent() {
    return agent;
  }

  public void setAgent(String agent) {
    this.agent = agent;
  }

  public Boolean getHomecomingSeg() {
    return homecomingSeg;
  }

  public void setHomecomingSeg(Boolean homecomingSeg) {
    this.homecomingSeg = homecomingSeg;
  }

  @Override
  public String toString() {
    return "DepartSegFactModel{" +
            "pkId='" + pkId + '\'' +
            ", tikNum='" + tikNum + '\'' +
            ", fkDeparturesDate='" + fkDeparturesDate + '\'' +
            ", fkArriveDate='" + fkArriveDate + '\'' +
            ", fkDeparturesTime='" + fkDeparturesTime + '\'' +
            ", fkArriveTime='" + fkArriveTime + '\'' +
            ", fkPnrdSeg='" + fkPnrdSeg + '\'' +
            ", fkDepairport='" + fkDepairport + '\'' +
            ", fkArriairport='" + fkArriairport + '\'' +
            ", passengerType='" + passengerType + '\'' +
            ", enLastName='" + enLastName + '\'' +
            ", enFirstName='" + enFirstName + '\'' +
            ", cnName='" + cnName + '\'' +
            ", certType='" + certType + '\'' +
            ", revisedCertType='" + revisedCertType + '\'' +
            ", certNumber='" + certNumber + '\'' +
            ", passengerAge=" + passengerAge +
            ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
            ", akSegcabin='" + akSegcabin + '\'' +
            ", akCabin='" + akCabin + '\'' +
            ", akDimark='" + akDimark + '\'' +
            ", fkIssueDate='" + fkIssueDate + '\'' +
            ", fkIssueTime='" + fkIssueTime + '\'' +
            ", ffrf='" + ffrf + '\'' +
            ", isUsedFfr=" + isUsedFfr +
            ", ffLevel='" + ffLevel + '\'' +
            ", ffAirline='" + ffAirline + '\'' +
            ", ffAllianceLevel='" + ffAllianceLevel + '\'' +
            ", transportCurrency='" + transportCurrency + '\'' +
            ", segTransportTikPrice=" + segTransportTikPrice +
            ", segTransportAgencyFee=" + segTransportAgencyFee +
            ", segTransportNetAmount=" + segTransportNetAmount +
            ", agent='" + agent + '\'' +
            ", stopover='" + stopover + '\'' +
            ", stopoverDay=" + stopoverDay +
            ", akAdvbookDay=" + akAdvbookDay +
            ", peerChd=" + peerChd +
            ", peerSenior=" + peerSenior +
            ", selfBooking=" + selfBooking +
            ", firstTraveler=" + firstTraveler +
            ", akTeammark=" + akTeammark +
            ", homecomingSeg=" + homecomingSeg +
            ", segCount=" + segCount +
            ", isKeyAccount=" + isKeyAccount +
            ", dataActive=" + dataActive +
            ", dataActiveTime='" + dataActiveTime + '\'' +
            ", systemCreatetime='" + systemCreatetime + '\'' +
            ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
            '}';
  }
}
