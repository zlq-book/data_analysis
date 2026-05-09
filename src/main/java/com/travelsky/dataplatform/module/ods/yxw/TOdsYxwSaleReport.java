package com.travelsky.dataplatform.module.ods.yxw;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.math.BigDecimal;

/**
 * 电子票销售报告表
 */
public class TOdsYxwSaleReport implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 主键ID
     */
    @ExcelIgnore
    private Long id;

    @ExcelProperty("SDASRC")
    private String sdasrc;

    @ExcelProperty("SDASTA")
    private String sdasta;

    @ExcelProperty("电子票类型")
    private String electronicTicketType;

    @ExcelProperty("代理号")
    private String agentCode;

    @ExcelProperty("代理名称")
    private String agentName;

    @ExcelProperty("销售类型")
    private String saleType;

    @ExcelProperty("原票票号")
    private String originalTicketNumber;

    @ExcelProperty("销售处理月")
    private String saleProcessMonth;

    @ExcelProperty("销售日期")
    private String saleDate;

    @ExcelProperty("票号")
    private String ticketNumber;

    @ExcelProperty("票面航程")
    private String ticketRoute;

    @ExcelProperty("航程类型")
    private String routeType;

    @ExcelProperty("TOURCODE")
    private String tourCode;

    @ExcelProperty("外航承运标识")
    private String foreignCarrierFlag;

    @ExcelProperty("团散标记")
    private String groupIndividualFlag;

    @ExcelProperty("团队名称")
    private String teamName;

    @ExcelProperty("旅客类型")
    private String passengerType;

    @ExcelProperty("联票标识")
    private String linkedTicketFlag;

    @ExcelProperty("常旅客号")
    private String frequentFlyerNumber;

    @ExcelProperty("出票GDS")
    private String ticketGds;

    @ExcelProperty("签注栏")
    private String endorsement;

    @ExcelProperty("毛额")
    private BigDecimal grossAmount;

    @ExcelProperty("净额")
    private BigDecimal netAmount;

    @ExcelProperty("标准代理费率")
    private BigDecimal standardAgentRate;

    @ExcelProperty("标准代理费")
    private BigDecimal standardAgentFee;

    @ExcelProperty("额外代理费率")
    private BigDecimal extraAgentRate;

    @ExcelProperty("额外代理费")
    private BigDecimal extraAgentFee;

    // 航段1信息
    @ExcelProperty("航段序号1")
    private String segmentNo1;

    @ExcelProperty("经停1")
    private String stopover1;

    @ExcelProperty("MC承运1")
    private String mcCarrier1;

    @ExcelProperty("MC航班1")
    private String mcFlight1;

    @ExcelProperty("OC承运1")
    private String ocCarrier1;

    @ExcelProperty("OC航班1")
    private String ocFlight1;

    @ExcelProperty("销售航段类型1")
    private String saleSegmentType1;

    @ExcelProperty("本外航承运标识1")
    private String domesticForeignCarrierFlag1;

    @ExcelProperty("实际承运人1")
    private String actualCarrier1;

    @ExcelProperty("实际承运航班号1")
    private String actualCarrierFlight1;

    @ExcelProperty("起飞时刻1")
    private String departureTime1;

    @ExcelProperty("票面航段1")
    private String ticketSegment1;

    @ExcelProperty("实际承运航段1")
    private String actualCarrierSegment1;

    @ExcelProperty("销售票面旅行日期1")
    private String ticketTravelDate1;

    @ExcelProperty("运输处理月1")
    private String transportProcessMonth1;

    @ExcelProperty("实际承运日期1")
    private String actualCarrierDate1;

    @ExcelProperty("运输航段类型1")
    private String transportSegmentType1;

    @ExcelProperty("销售票面舱位1")
    private String ticketCabin1;

    @ExcelProperty("实际运输舱位1")
    private String actualCabin1;

    @ExcelProperty("销售FBA1")
    private String saleFba1;

    @ExcelProperty("运输FBA1")
    private String transportFba1;

    @ExcelProperty("销售分摊1")
    private BigDecimal saleApportionment1;

    @ExcelProperty("销售燃油1")
    private BigDecimal saleFuel1;

    @ExcelProperty("运输分摊额1CNY")
    private BigDecimal transportApportionment1;

    @ExcelProperty("运输燃油1")
    private BigDecimal transportFuel1;

    @ExcelProperty("销售分摊手续费1")
    private BigDecimal saleCommission1;

    // 航段2信息
    @ExcelProperty("航段序号2")
    private String segmentNo2;

    @ExcelProperty("经停2")
    private String stopover2;

    @ExcelProperty("MC承运2")
    private String mcCarrier2;

    @ExcelProperty("MC航班2")
    private String mcFlight2;

    @ExcelProperty("OC承运2")
    private String ocCarrier2;

    @ExcelProperty("OC航班2")
    private String ocFlight2;

    @ExcelProperty("销售航段类型2")
    private String saleSegmentType2;

    @ExcelProperty("本外航承运标识2")
    private String domesticForeignCarrierFlag2;

    @ExcelProperty("实际承运人2")
    private String actualCarrier2;

    @ExcelProperty("实际承运航班号2")
    private String actualCarrierFlight2;

    @ExcelProperty("起飞时刻2")
    private String departureTime2;

    @ExcelProperty("销售票面航段2")
    private String ticketSegment2;

    @ExcelProperty("实际承运航段CPN2")
    private String actualCarrierSegmentCpn2;

    @ExcelProperty("销售票面旅行日期2")
    private String ticketTravelDate2;

    @ExcelProperty("运输处理月2")
    private String transportProcessMonth2;

    @ExcelProperty("实际承运日期CPN2")
    private String actualCarrierDateCpn2;

    @ExcelProperty("运输航段类型CPN2")
    private String transportSegmentTypeCpn2;

    @ExcelProperty("销售票面舱位2")
    private String ticketCabin2;

    @ExcelProperty("实际运输舱位CPN2")
    private String actualCabinCpn2;

    @ExcelProperty("销售FBA2")
    private String saleFba2;

    @ExcelProperty("运输FBA2")
    private String transportFba2;

    @ExcelProperty("销售分摊2")
    private BigDecimal saleApportionment2;

    @ExcelProperty("销售燃油2")
    private BigDecimal saleFuel2;

    @ExcelProperty("运输分摊额2CNY")
    private BigDecimal transportApportionment2;

    @ExcelProperty("运输燃油2")
    private BigDecimal transportFuel2;

    @ExcelProperty("销售分摊手续费2")
    private BigDecimal saleCommission2;

    // 航段3信息
    @ExcelProperty("航段序号3")
    private String segmentNo3;

    @ExcelProperty("经停3")
    private String stopover3;

    @ExcelProperty("MC承运3")
    private String mcCarrier3;

    @ExcelProperty("MC航班3")
    private String mcFlight3;

    @ExcelProperty("OC承运3")
    private String ocCarrier3;

    @ExcelProperty("OC航班3")
    private String ocFlight3;

    @ExcelProperty("销售航段类型3")
    private String saleSegmentType3;

    @ExcelProperty("本外航承运标识3")
    private String domesticForeignCarrierFlag3;

    @ExcelProperty("实际承运人3")
    private String actualCarrier3;

    @ExcelProperty("实际承运航班号3")
    private String actualCarrierFlight3;

    @ExcelProperty("CPN3起飞时刻")
    private String departureTimeCpn3;

    @ExcelProperty("票面航段CPN3")
    private String ticketSegmentCpn3;

    @ExcelProperty("实际承运航段CPN3")
    private String actualCarrierSegmentCpn3;

    @ExcelProperty("销售票面旅行日期3")
    private String ticketTravelDate3;

    @ExcelProperty("运输处理月3")
    private String transportProcessMonth3;

    @ExcelProperty("实际承运日期3")
    private String actualCarrierDate3;

    @ExcelProperty("运输航段类型3")
    private String transportSegmentType3;

    @ExcelProperty("销售票面舱位3")
    private String ticketCabin3;

    @ExcelProperty("实际运输舱位3")
    private String actualCabin3;

    @ExcelProperty("销售FBA3")
    private String saleFba3;

    @ExcelProperty("运输FBA3")
    private String transportFba3;

    @ExcelProperty("销售分摊3")
    private BigDecimal saleApportionment3;

    @ExcelProperty("销售燃油3")
    private BigDecimal saleFuel3;

    @ExcelProperty("运输分摊额3CNY")
    private BigDecimal transportApportionment3;

    @ExcelProperty("运输燃油3")
    private BigDecimal transportFuel3;

    @ExcelProperty("销售分摊手续费3")
    private BigDecimal saleCommission3;

    // 航段4信息
    @ExcelProperty("航段序号4")
    private String segmentNo4;

    @ExcelProperty("经停4")
    private String stopover4;

    @ExcelProperty("MC承运4")
    private String mcCarrier4;

    @ExcelProperty("MC航班4")
    private String mcFlight4;

    @ExcelProperty("OC承运4")
    private String ocCarrier4;

    @ExcelProperty("OC航班4")
    private String ocFlight4;

    @ExcelProperty("销售航段类型4")
    private String saleSegmentType4;

    @ExcelProperty("本外航承运标识4")
    private String domesticForeignCarrierFlag4;

    @ExcelProperty("实际承运人4")
    private String actualCarrier4;

    @ExcelProperty("实际承运航班号4")
    private String actualCarrierFlight4;

    @ExcelProperty("CPN4起飞时刻")
    private String departureTimeCpn4;

    @ExcelProperty("票面航段4")
    private String ticketSegment4;

    @ExcelProperty("实际承运航段4")
    private String actualCarrierSegment4;

    @ExcelProperty("销售票面旅行日期4")
    private String ticketTravelDate4;

    @ExcelProperty("运输处理月4")
    private String transportProcessMonth4;

    @ExcelProperty("实际承运日期4")
    private String actualCarrierDate4;

    @ExcelProperty("运输航段类型4")
    private String transportSegmentType4;

    @ExcelProperty("销售票面舱位4")
    private String ticketCabin4;

    @ExcelProperty("实际运输舱位4")
    private String actualCabin4;

    @ExcelProperty("销售FBA4")
    private String saleFba4;

    @ExcelProperty("运输FBA4")
    private String transportFba4;

    @ExcelProperty("销售分摊4")
    private BigDecimal saleApportionment4;

    @ExcelProperty("销售燃油4")
    private BigDecimal saleFuel4;

    @ExcelProperty("运输分摊额4CNY")
    private BigDecimal transportApportionment4;

    @ExcelProperty("运输燃油4")
    private BigDecimal transportFuel4;

    @ExcelProperty("销售分摊手续费4")
    private BigDecimal saleCommission4;

    /**
     * 数据入仓时间
     */
    @ExcelIgnore
    private String etlCreateTime;

    /**
     * 数据在数仓更新时间
     */
    @ExcelIgnore
    private String etlUpdateTime;

    /**
     * 数据ETL日期
     */
    @ExcelIgnore
    private String etlDate;

    public TOdsYxwSaleReport() {
    }


    public Long getId() {
        return this.id;
    }

    public String getSdasrc() {
        return this.sdasrc;
    }

    public String getSdasta() {
        return this.sdasta;
    }

    public String getElectronicTicketType() {
        return this.electronicTicketType;
    }

    public String getAgentCode() {
        return this.agentCode;
    }

    public String getAgentName() {
        return this.agentName;
    }

    public String getSaleType() {
        return this.saleType;
    }

    public String getOriginalTicketNumber() {
        return this.originalTicketNumber;
    }

    public String getSaleProcessMonth() {
        return this.saleProcessMonth;
    }

    public String getSaleDate() {
        return this.saleDate;
    }

    public String getTicketNumber() {
        return this.ticketNumber;
    }

    public String getTicketRoute() {
        return this.ticketRoute;
    }

    public String getRouteType() {
        return this.routeType;
    }

    public String getTourCode() {
        return this.tourCode;
    }

    public String getForeignCarrierFlag() {
        return this.foreignCarrierFlag;
    }

    public String getGroupIndividualFlag() {
        return this.groupIndividualFlag;
    }

    public String getTeamName() {
        return this.teamName;
    }

    public String getPassengerType() {
        return this.passengerType;
    }

    public String getLinkedTicketFlag() {
        return this.linkedTicketFlag;
    }

    public String getFrequentFlyerNumber() {
        return this.frequentFlyerNumber;
    }

    public String getTicketGds() {
        return this.ticketGds;
    }

    public String getEndorsement() {
        return this.endorsement;
    }

    public BigDecimal getGrossAmount() {
        return this.grossAmount;
    }

    public BigDecimal getNetAmount() {
        return this.netAmount;
    }

    public BigDecimal getStandardAgentRate() {
        return this.standardAgentRate;
    }

    public BigDecimal getStandardAgentFee() {
        return this.standardAgentFee;
    }

    public BigDecimal getExtraAgentRate() {
        return this.extraAgentRate;
    }

    public BigDecimal getExtraAgentFee() {
        return this.extraAgentFee;
    }

    public String getSegmentNo1() {
        return this.segmentNo1;
    }

    public String getStopover1() {
        return this.stopover1;
    }

    public String getMcCarrier1() {
        return this.mcCarrier1;
    }

    public String getMcFlight1() {
        return this.mcFlight1;
    }

    public String getOcCarrier1() {
        return this.ocCarrier1;
    }

    public String getOcFlight1() {
        return this.ocFlight1;
    }

    public String getSaleSegmentType1() {
        return this.saleSegmentType1;
    }

    public String getDomesticForeignCarrierFlag1() {
        return this.domesticForeignCarrierFlag1;
    }

    public String getActualCarrier1() {
        return this.actualCarrier1;
    }

    public String getActualCarrierFlight1() {
        return this.actualCarrierFlight1;
    }

    public String getDepartureTime1() {
        return this.departureTime1;
    }

    public String getTicketSegment1() {
        return this.ticketSegment1;
    }

    public String getActualCarrierSegment1() {
        return this.actualCarrierSegment1;
    }

    public String getTicketTravelDate1() {
        return this.ticketTravelDate1;
    }

    public String getTransportProcessMonth1() {
        return this.transportProcessMonth1;
    }

    public String getActualCarrierDate1() {
        return this.actualCarrierDate1;
    }

    public String getTransportSegmentType1() {
        return this.transportSegmentType1;
    }

    public String getTicketCabin1() {
        return this.ticketCabin1;
    }

    public String getActualCabin1() {
        return this.actualCabin1;
    }

    public String getSaleFba1() {
        return this.saleFba1;
    }

    public String getTransportFba1() {
        return this.transportFba1;
    }

    public BigDecimal getSaleApportionment1() {
        return this.saleApportionment1;
    }

    public BigDecimal getSaleFuel1() {
        return this.saleFuel1;
    }

    public BigDecimal getTransportApportionment1() {
        return this.transportApportionment1;
    }

    public BigDecimal getTransportFuel1() {
        return this.transportFuel1;
    }

    public BigDecimal getSaleCommission1() {
        return this.saleCommission1;
    }

    public String getSegmentNo2() {
        return this.segmentNo2;
    }

    public String getStopover2() {
        return this.stopover2;
    }

    public String getMcCarrier2() {
        return this.mcCarrier2;
    }

    public String getMcFlight2() {
        return this.mcFlight2;
    }

    public String getOcCarrier2() {
        return this.ocCarrier2;
    }

    public String getOcFlight2() {
        return this.ocFlight2;
    }

    public String getSaleSegmentType2() {
        return this.saleSegmentType2;
    }

    public String getDomesticForeignCarrierFlag2() {
        return this.domesticForeignCarrierFlag2;
    }

    public String getActualCarrier2() {
        return this.actualCarrier2;
    }

    public String getActualCarrierFlight2() {
        return this.actualCarrierFlight2;
    }

    public String getDepartureTime2() {
        return this.departureTime2;
    }

    public String getTicketSegment2() {
        return this.ticketSegment2;
    }

    public String getActualCarrierSegmentCpn2() {
        return this.actualCarrierSegmentCpn2;
    }

    public String getTicketTravelDate2() {
        return this.ticketTravelDate2;
    }

    public String getTransportProcessMonth2() {
        return this.transportProcessMonth2;
    }

    public String getActualCarrierDateCpn2() {
        return this.actualCarrierDateCpn2;
    }

    public String getTransportSegmentTypeCpn2() {
        return this.transportSegmentTypeCpn2;
    }

    public String getTicketCabin2() {
        return this.ticketCabin2;
    }

    public String getActualCabinCpn2() {
        return this.actualCabinCpn2;
    }

    public String getSaleFba2() {
        return this.saleFba2;
    }

    public String getTransportFba2() {
        return this.transportFba2;
    }

    public BigDecimal getSaleApportionment2() {
        return this.saleApportionment2;
    }

    public BigDecimal getSaleFuel2() {
        return this.saleFuel2;
    }

    public BigDecimal getTransportApportionment2() {
        return this.transportApportionment2;
    }

    public BigDecimal getTransportFuel2() {
        return this.transportFuel2;
    }

    public BigDecimal getSaleCommission2() {
        return this.saleCommission2;
    }

    public String getSegmentNo3() {
        return this.segmentNo3;
    }

    public String getStopover3() {
        return this.stopover3;
    }

    public String getMcCarrier3() {
        return this.mcCarrier3;
    }

    public String getMcFlight3() {
        return this.mcFlight3;
    }

    public String getOcCarrier3() {
        return this.ocCarrier3;
    }

    public String getOcFlight3() {
        return this.ocFlight3;
    }

    public String getSaleSegmentType3() {
        return this.saleSegmentType3;
    }

    public String getDomesticForeignCarrierFlag3() {
        return this.domesticForeignCarrierFlag3;
    }

    public String getActualCarrier3() {
        return this.actualCarrier3;
    }

    public String getActualCarrierFlight3() {
        return this.actualCarrierFlight3;
    }

    public String getDepartureTimeCpn3() {
        return this.departureTimeCpn3;
    }

    public String getTicketSegmentCpn3() {
        return this.ticketSegmentCpn3;
    }

    public String getActualCarrierSegmentCpn3() {
        return this.actualCarrierSegmentCpn3;
    }

    public String getTicketTravelDate3() {
        return this.ticketTravelDate3;
    }

    public String getTransportProcessMonth3() {
        return this.transportProcessMonth3;
    }

    public String getActualCarrierDate3() {
        return this.actualCarrierDate3;
    }

    public String getTransportSegmentType3() {
        return this.transportSegmentType3;
    }

    public String getTicketCabin3() {
        return this.ticketCabin3;
    }

    public String getActualCabin3() {
        return this.actualCabin3;
    }

    public String getSaleFba3() {
        return this.saleFba3;
    }

    public String getTransportFba3() {
        return this.transportFba3;
    }

    public BigDecimal getSaleApportionment3() {
        return this.saleApportionment3;
    }

    public BigDecimal getSaleFuel3() {
        return this.saleFuel3;
    }

    public BigDecimal getTransportApportionment3() {
        return this.transportApportionment3;
    }

    public BigDecimal getTransportFuel3() {
        return this.transportFuel3;
    }

    public BigDecimal getSaleCommission3() {
        return this.saleCommission3;
    }

    public String getSegmentNo4() {
        return this.segmentNo4;
    }

    public String getStopover4() {
        return this.stopover4;
    }

    public String getMcCarrier4() {
        return this.mcCarrier4;
    }

    public String getMcFlight4() {
        return this.mcFlight4;
    }

    public String getOcCarrier4() {
        return this.ocCarrier4;
    }

    public String getOcFlight4() {
        return this.ocFlight4;
    }

    public String getSaleSegmentType4() {
        return this.saleSegmentType4;
    }

    public String getDomesticForeignCarrierFlag4() {
        return this.domesticForeignCarrierFlag4;
    }

    public String getActualCarrier4() {
        return this.actualCarrier4;
    }

    public String getActualCarrierFlight4() {
        return this.actualCarrierFlight4;
    }

    public String getDepartureTimeCpn4() {
        return this.departureTimeCpn4;
    }

    public String getTicketSegment4() {
        return this.ticketSegment4;
    }

    public String getActualCarrierSegment4() {
        return this.actualCarrierSegment4;
    }

    public String getTicketTravelDate4() {
        return this.ticketTravelDate4;
    }

    public String getTransportProcessMonth4() {
        return this.transportProcessMonth4;
    }

    public String getActualCarrierDate4() {
        return this.actualCarrierDate4;
    }

    public String getTransportSegmentType4() {
        return this.transportSegmentType4;
    }

    public String getTicketCabin4() {
        return this.ticketCabin4;
    }

    public String getActualCabin4() {
        return this.actualCabin4;
    }

    public String getSaleFba4() {
        return this.saleFba4;
    }

    public String getTransportFba4() {
        return this.transportFba4;
    }

    public BigDecimal getSaleApportionment4() {
        return this.saleApportionment4;
    }

    public BigDecimal getSaleFuel4() {
        return this.saleFuel4;
    }

    public BigDecimal getTransportApportionment4() {
        return this.transportApportionment4;
    }

    public BigDecimal getTransportFuel4() {
        return this.transportFuel4;
    }

    public BigDecimal getSaleCommission4() {
        return this.saleCommission4;
    }

    public String getEtlCreateTime() {
        return this.etlCreateTime;
    }

    public String getEtlUpdateTime() {
        return this.etlUpdateTime;
    }

    public String getEtlDate() {
        return this.etlDate;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setSdasrc(String sdasrc) {
        this.sdasrc = sdasrc;
    }

    public void setSdasta(String sdasta) {
        this.sdasta = sdasta;
    }

    public void setElectronicTicketType(String electronicTicketType) {
        this.electronicTicketType = electronicTicketType;
    }

    public void setAgentCode(String agentCode) {
        this.agentCode = agentCode;
    }

    public void setAgentName(String agentName) {
        this.agentName = agentName;
    }

    public void setSaleType(String saleType) {
        this.saleType = saleType;
    }

    public void setOriginalTicketNumber(String originalTicketNumber) {
        this.originalTicketNumber = originalTicketNumber;
    }

    public void setSaleProcessMonth(String saleProcessMonth) {
        this.saleProcessMonth = saleProcessMonth;
    }

    public void setSaleDate(String saleDate) {
        this.saleDate = saleDate;
    }

    public void setTicketNumber(String ticketNumber) {
        this.ticketNumber = ticketNumber;
    }

    public void setTicketRoute(String ticketRoute) {
        this.ticketRoute = ticketRoute;
    }

    public void setRouteType(String routeType) {
        this.routeType = routeType;
    }

    public void setTourCode(String tourCode) {
        this.tourCode = tourCode;
    }

    public void setForeignCarrierFlag(String foreignCarrierFlag) {
        this.foreignCarrierFlag = foreignCarrierFlag;
    }

    public void setGroupIndividualFlag(String groupIndividualFlag) {
        this.groupIndividualFlag = groupIndividualFlag;
    }

    public void setTeamName(String teamName) {
        this.teamName = teamName;
    }

    public void setPassengerType(String passengerType) {
        this.passengerType = passengerType;
    }

    public void setLinkedTicketFlag(String linkedTicketFlag) {
        this.linkedTicketFlag = linkedTicketFlag;
    }

    public void setFrequentFlyerNumber(String frequentFlyerNumber) {
        this.frequentFlyerNumber = frequentFlyerNumber;
    }

    public void setTicketGds(String ticketGds) {
        this.ticketGds = ticketGds;
    }

    public void setEndorsement(String endorsement) {
        this.endorsement = endorsement;
    }

    public void setGrossAmount(BigDecimal grossAmount) {
        this.grossAmount = grossAmount;
    }

    public void setNetAmount(BigDecimal netAmount) {
        this.netAmount = netAmount;
    }

    public void setStandardAgentRate(BigDecimal standardAgentRate) {
        this.standardAgentRate = standardAgentRate;
    }

    public void setStandardAgentFee(BigDecimal standardAgentFee) {
        this.standardAgentFee = standardAgentFee;
    }

    public void setExtraAgentRate(BigDecimal extraAgentRate) {
        this.extraAgentRate = extraAgentRate;
    }

    public void setExtraAgentFee(BigDecimal extraAgentFee) {
        this.extraAgentFee = extraAgentFee;
    }

    public void setSegmentNo1(String segmentNo1) {
        this.segmentNo1 = segmentNo1;
    }

    public void setStopover1(String stopover1) {
        this.stopover1 = stopover1;
    }

    public void setMcCarrier1(String mcCarrier1) {
        this.mcCarrier1 = mcCarrier1;
    }

    public void setMcFlight1(String mcFlight1) {
        this.mcFlight1 = mcFlight1;
    }

    public void setOcCarrier1(String ocCarrier1) {
        this.ocCarrier1 = ocCarrier1;
    }

    public void setOcFlight1(String ocFlight1) {
        this.ocFlight1 = ocFlight1;
    }

    public void setSaleSegmentType1(String saleSegmentType1) {
        this.saleSegmentType1 = saleSegmentType1;
    }

    public void setDomesticForeignCarrierFlag1(String domesticForeignCarrierFlag1) {
        this.domesticForeignCarrierFlag1 = domesticForeignCarrierFlag1;
    }

    public void setActualCarrier1(String actualCarrier1) {
        this.actualCarrier1 = actualCarrier1;
    }

    public void setActualCarrierFlight1(String actualCarrierFlight1) {
        this.actualCarrierFlight1 = actualCarrierFlight1;
    }

    public void setDepartureTime1(String departureTime1) {
        this.departureTime1 = departureTime1;
    }

    public void setTicketSegment1(String ticketSegment1) {
        this.ticketSegment1 = ticketSegment1;
    }

    public void setActualCarrierSegment1(String actualCarrierSegment1) {
        this.actualCarrierSegment1 = actualCarrierSegment1;
    }

    public void setTicketTravelDate1(String ticketTravelDate1) {
        this.ticketTravelDate1 = ticketTravelDate1;
    }

    public void setTransportProcessMonth1(String transportProcessMonth1) {
        this.transportProcessMonth1 = transportProcessMonth1;
    }

    public void setActualCarrierDate1(String actualCarrierDate1) {
        this.actualCarrierDate1 = actualCarrierDate1;
    }

    public void setTransportSegmentType1(String transportSegmentType1) {
        this.transportSegmentType1 = transportSegmentType1;
    }

    public void setTicketCabin1(String ticketCabin1) {
        this.ticketCabin1 = ticketCabin1;
    }

    public void setActualCabin1(String actualCabin1) {
        this.actualCabin1 = actualCabin1;
    }

    public void setSaleFba1(String saleFba1) {
        this.saleFba1 = saleFba1;
    }

    public void setTransportFba1(String transportFba1) {
        this.transportFba1 = transportFba1;
    }

    public void setSaleApportionment1(BigDecimal saleApportionment1) {
        this.saleApportionment1 = saleApportionment1;
    }

    public void setSaleFuel1(BigDecimal saleFuel1) {
        this.saleFuel1 = saleFuel1;
    }

    public void setTransportApportionment1(BigDecimal transportApportionment1) {
        this.transportApportionment1 = transportApportionment1;
    }

    public void setTransportFuel1(BigDecimal transportFuel1) {
        this.transportFuel1 = transportFuel1;
    }

    public void setSaleCommission1(BigDecimal saleCommission1) {
        this.saleCommission1 = saleCommission1;
    }

    public void setSegmentNo2(String segmentNo2) {
        this.segmentNo2 = segmentNo2;
    }

    public void setStopover2(String stopover2) {
        this.stopover2 = stopover2;
    }

    public void setMcCarrier2(String mcCarrier2) {
        this.mcCarrier2 = mcCarrier2;
    }

    public void setMcFlight2(String mcFlight2) {
        this.mcFlight2 = mcFlight2;
    }

    public void setOcCarrier2(String ocCarrier2) {
        this.ocCarrier2 = ocCarrier2;
    }

    public void setOcFlight2(String ocFlight2) {
        this.ocFlight2 = ocFlight2;
    }

    public void setSaleSegmentType2(String saleSegmentType2) {
        this.saleSegmentType2 = saleSegmentType2;
    }

    public void setDomesticForeignCarrierFlag2(String domesticForeignCarrierFlag2) {
        this.domesticForeignCarrierFlag2 = domesticForeignCarrierFlag2;
    }

    public void setActualCarrier2(String actualCarrier2) {
        this.actualCarrier2 = actualCarrier2;
    }

    public void setActualCarrierFlight2(String actualCarrierFlight2) {
        this.actualCarrierFlight2 = actualCarrierFlight2;
    }

    public void setDepartureTime2(String departureTime2) {
        this.departureTime2 = departureTime2;
    }

    public void setTicketSegment2(String ticketSegment2) {
        this.ticketSegment2 = ticketSegment2;
    }

    public void setActualCarrierSegmentCpn2(String actualCarrierSegmentCpn2) {
        this.actualCarrierSegmentCpn2 = actualCarrierSegmentCpn2;
    }

    public void setTicketTravelDate2(String ticketTravelDate2) {
        this.ticketTravelDate2 = ticketTravelDate2;
    }

    public void setTransportProcessMonth2(String transportProcessMonth2) {
        this.transportProcessMonth2 = transportProcessMonth2;
    }

    public void setActualCarrierDateCpn2(String actualCarrierDateCpn2) {
        this.actualCarrierDateCpn2 = actualCarrierDateCpn2;
    }

    public void setTransportSegmentTypeCpn2(String transportSegmentTypeCpn2) {
        this.transportSegmentTypeCpn2 = transportSegmentTypeCpn2;
    }

    public void setTicketCabin2(String ticketCabin2) {
        this.ticketCabin2 = ticketCabin2;
    }

    public void setActualCabinCpn2(String actualCabinCpn2) {
        this.actualCabinCpn2 = actualCabinCpn2;
    }

    public void setSaleFba2(String saleFba2) {
        this.saleFba2 = saleFba2;
    }

    public void setTransportFba2(String transportFba2) {
        this.transportFba2 = transportFba2;
    }

    public void setSaleApportionment2(BigDecimal saleApportionment2) {
        this.saleApportionment2 = saleApportionment2;
    }

    public void setSaleFuel2(BigDecimal saleFuel2) {
        this.saleFuel2 = saleFuel2;
    }

    public void setTransportApportionment2(BigDecimal transportApportionment2) {
        this.transportApportionment2 = transportApportionment2;
    }

    public void setTransportFuel2(BigDecimal transportFuel2) {
        this.transportFuel2 = transportFuel2;
    }

    public void setSaleCommission2(BigDecimal saleCommission2) {
        this.saleCommission2 = saleCommission2;
    }

    public void setSegmentNo3(String segmentNo3) {
        this.segmentNo3 = segmentNo3;
    }

    public void setStopover3(String stopover3) {
        this.stopover3 = stopover3;
    }

    public void setMcCarrier3(String mcCarrier3) {
        this.mcCarrier3 = mcCarrier3;
    }

    public void setMcFlight3(String mcFlight3) {
        this.mcFlight3 = mcFlight3;
    }

    public void setOcCarrier3(String ocCarrier3) {
        this.ocCarrier3 = ocCarrier3;
    }

    public void setOcFlight3(String ocFlight3) {
        this.ocFlight3 = ocFlight3;
    }

    public void setSaleSegmentType3(String saleSegmentType3) {
        this.saleSegmentType3 = saleSegmentType3;
    }

    public void setDomesticForeignCarrierFlag3(String domesticForeignCarrierFlag3) {
        this.domesticForeignCarrierFlag3 = domesticForeignCarrierFlag3;
    }

    public void setActualCarrier3(String actualCarrier3) {
        this.actualCarrier3 = actualCarrier3;
    }

    public void setActualCarrierFlight3(String actualCarrierFlight3) {
        this.actualCarrierFlight3 = actualCarrierFlight3;
    }

    public void setDepartureTimeCpn3(String departureTimeCpn3) {
        this.departureTimeCpn3 = departureTimeCpn3;
    }

    public void setTicketSegmentCpn3(String ticketSegmentCpn3) {
        this.ticketSegmentCpn3 = ticketSegmentCpn3;
    }

    public void setActualCarrierSegmentCpn3(String actualCarrierSegmentCpn3) {
        this.actualCarrierSegmentCpn3 = actualCarrierSegmentCpn3;
    }

    public void setTicketTravelDate3(String ticketTravelDate3) {
        this.ticketTravelDate3 = ticketTravelDate3;
    }

    public void setTransportProcessMonth3(String transportProcessMonth3) {
        this.transportProcessMonth3 = transportProcessMonth3;
    }

    public void setActualCarrierDate3(String actualCarrierDate3) {
        this.actualCarrierDate3 = actualCarrierDate3;
    }

    public void setTransportSegmentType3(String transportSegmentType3) {
        this.transportSegmentType3 = transportSegmentType3;
    }

    public void setTicketCabin3(String ticketCabin3) {
        this.ticketCabin3 = ticketCabin3;
    }

    public void setActualCabin3(String actualCabin3) {
        this.actualCabin3 = actualCabin3;
    }

    public void setSaleFba3(String saleFba3) {
        this.saleFba3 = saleFba3;
    }

    public void setTransportFba3(String transportFba3) {
        this.transportFba3 = transportFba3;
    }

    public void setSaleApportionment3(BigDecimal saleApportionment3) {
        this.saleApportionment3 = saleApportionment3;
    }

    public void setSaleFuel3(BigDecimal saleFuel3) {
        this.saleFuel3 = saleFuel3;
    }

    public void setTransportApportionment3(BigDecimal transportApportionment3) {
        this.transportApportionment3 = transportApportionment3;
    }

    public void setTransportFuel3(BigDecimal transportFuel3) {
        this.transportFuel3 = transportFuel3;
    }

    public void setSaleCommission3(BigDecimal saleCommission3) {
        this.saleCommission3 = saleCommission3;
    }

    public void setSegmentNo4(String segmentNo4) {
        this.segmentNo4 = segmentNo4;
    }

    public void setStopover4(String stopover4) {
        this.stopover4 = stopover4;
    }

    public void setMcCarrier4(String mcCarrier4) {
        this.mcCarrier4 = mcCarrier4;
    }

    public void setMcFlight4(String mcFlight4) {
        this.mcFlight4 = mcFlight4;
    }

    public void setOcCarrier4(String ocCarrier4) {
        this.ocCarrier4 = ocCarrier4;
    }

    public void setOcFlight4(String ocFlight4) {
        this.ocFlight4 = ocFlight4;
    }

    public void setSaleSegmentType4(String saleSegmentType4) {
        this.saleSegmentType4 = saleSegmentType4;
    }

    public void setDomesticForeignCarrierFlag4(String domesticForeignCarrierFlag4) {
        this.domesticForeignCarrierFlag4 = domesticForeignCarrierFlag4;
    }

    public void setActualCarrier4(String actualCarrier4) {
        this.actualCarrier4 = actualCarrier4;
    }

    public void setActualCarrierFlight4(String actualCarrierFlight4) {
        this.actualCarrierFlight4 = actualCarrierFlight4;
    }

    public void setDepartureTimeCpn4(String departureTimeCpn4) {
        this.departureTimeCpn4 = departureTimeCpn4;
    }

    public void setTicketSegment4(String ticketSegment4) {
        this.ticketSegment4 = ticketSegment4;
    }

    public void setActualCarrierSegment4(String actualCarrierSegment4) {
        this.actualCarrierSegment4 = actualCarrierSegment4;
    }

    public void setTicketTravelDate4(String ticketTravelDate4) {
        this.ticketTravelDate4 = ticketTravelDate4;
    }

    public void setTransportProcessMonth4(String transportProcessMonth4) {
        this.transportProcessMonth4 = transportProcessMonth4;
    }

    public void setActualCarrierDate4(String actualCarrierDate4) {
        this.actualCarrierDate4 = actualCarrierDate4;
    }

    public void setTransportSegmentType4(String transportSegmentType4) {
        this.transportSegmentType4 = transportSegmentType4;
    }

    public void setTicketCabin4(String ticketCabin4) {
        this.ticketCabin4 = ticketCabin4;
    }

    public void setActualCabin4(String actualCabin4) {
        this.actualCabin4 = actualCabin4;
    }

    public void setSaleFba4(String saleFba4) {
        this.saleFba4 = saleFba4;
    }

    public void setTransportFba4(String transportFba4) {
        this.transportFba4 = transportFba4;
    }

    public void setSaleApportionment4(BigDecimal saleApportionment4) {
        this.saleApportionment4 = saleApportionment4;
    }

    public void setSaleFuel4(BigDecimal saleFuel4) {
        this.saleFuel4 = saleFuel4;
    }

    public void setTransportApportionment4(BigDecimal transportApportionment4) {
        this.transportApportionment4 = transportApportionment4;
    }

    public void setTransportFuel4(BigDecimal transportFuel4) {
        this.transportFuel4 = transportFuel4;
    }

    public void setSaleCommission4(BigDecimal saleCommission4) {
        this.saleCommission4 = saleCommission4;
    }

    public void setEtlCreateTime(String etlCreateTime) {
        this.etlCreateTime = etlCreateTime;
    }

    public void setEtlUpdateTime(String etlUpdateTime) {
        this.etlUpdateTime = etlUpdateTime;
    }

    public void setEtlDate(String etlDate) {
        this.etlDate = etlDate;
    }

    public boolean equals(final Object o) {
        if (o == this) return true;
        if (!(o instanceof TOdsYxwSaleReport)) return false;
        final TOdsYxwSaleReport other = (TOdsYxwSaleReport) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$id = this.getId();
        final Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
        final Object this$sdasrc = this.getSdasrc();
        final Object other$sdasrc = other.getSdasrc();
        if (this$sdasrc == null ? other$sdasrc != null : !this$sdasrc.equals(other$sdasrc)) return false;
        final Object this$sdasta = this.getSdasta();
        final Object other$sdasta = other.getSdasta();
        if (this$sdasta == null ? other$sdasta != null : !this$sdasta.equals(other$sdasta)) return false;
        final Object this$electronicTicketType = this.getElectronicTicketType();
        final Object other$electronicTicketType = other.getElectronicTicketType();
        if (this$electronicTicketType == null ? other$electronicTicketType != null : !this$electronicTicketType.equals(other$electronicTicketType))
            return false;
        final Object this$agentCode = this.getAgentCode();
        final Object other$agentCode = other.getAgentCode();
        if (this$agentCode == null ? other$agentCode != null : !this$agentCode.equals(other$agentCode)) return false;
        final Object this$agentName = this.getAgentName();
        final Object other$agentName = other.getAgentName();
        if (this$agentName == null ? other$agentName != null : !this$agentName.equals(other$agentName)) return false;
        final Object this$saleType = this.getSaleType();
        final Object other$saleType = other.getSaleType();
        if (this$saleType == null ? other$saleType != null : !this$saleType.equals(other$saleType)) return false;
        final Object this$originalTicketNumber = this.getOriginalTicketNumber();
        final Object other$originalTicketNumber = other.getOriginalTicketNumber();
        if (this$originalTicketNumber == null ? other$originalTicketNumber != null : !this$originalTicketNumber.equals(other$originalTicketNumber))
            return false;
        final Object this$saleProcessMonth = this.getSaleProcessMonth();
        final Object other$saleProcessMonth = other.getSaleProcessMonth();
        if (this$saleProcessMonth == null ? other$saleProcessMonth != null : !this$saleProcessMonth.equals(other$saleProcessMonth))
            return false;
        final Object this$saleDate = this.getSaleDate();
        final Object other$saleDate = other.getSaleDate();
        if (this$saleDate == null ? other$saleDate != null : !this$saleDate.equals(other$saleDate)) return false;
        final Object this$ticketNumber = this.getTicketNumber();
        final Object other$ticketNumber = other.getTicketNumber();
        if (this$ticketNumber == null ? other$ticketNumber != null : !this$ticketNumber.equals(other$ticketNumber))
            return false;
        final Object this$ticketRoute = this.getTicketRoute();
        final Object other$ticketRoute = other.getTicketRoute();
        if (this$ticketRoute == null ? other$ticketRoute != null : !this$ticketRoute.equals(other$ticketRoute))
            return false;
        final Object this$routeType = this.getRouteType();
        final Object other$routeType = other.getRouteType();
        if (this$routeType == null ? other$routeType != null : !this$routeType.equals(other$routeType)) return false;
        final Object this$tourCode = this.getTourCode();
        final Object other$tourCode = other.getTourCode();
        if (this$tourCode == null ? other$tourCode != null : !this$tourCode.equals(other$tourCode)) return false;
        final Object this$foreignCarrierFlag = this.getForeignCarrierFlag();
        final Object other$foreignCarrierFlag = other.getForeignCarrierFlag();
        if (this$foreignCarrierFlag == null ? other$foreignCarrierFlag != null : !this$foreignCarrierFlag.equals(other$foreignCarrierFlag))
            return false;
        final Object this$groupIndividualFlag = this.getGroupIndividualFlag();
        final Object other$groupIndividualFlag = other.getGroupIndividualFlag();
        if (this$groupIndividualFlag == null ? other$groupIndividualFlag != null : !this$groupIndividualFlag.equals(other$groupIndividualFlag))
            return false;
        final Object this$teamName = this.getTeamName();
        final Object other$teamName = other.getTeamName();
        if (this$teamName == null ? other$teamName != null : !this$teamName.equals(other$teamName)) return false;
        final Object this$passengerType = this.getPassengerType();
        final Object other$passengerType = other.getPassengerType();
        if (this$passengerType == null ? other$passengerType != null : !this$passengerType.equals(other$passengerType))
            return false;
        final Object this$linkedTicketFlag = this.getLinkedTicketFlag();
        final Object other$linkedTicketFlag = other.getLinkedTicketFlag();
        if (this$linkedTicketFlag == null ? other$linkedTicketFlag != null : !this$linkedTicketFlag.equals(other$linkedTicketFlag))
            return false;
        final Object this$frequentFlyerNumber = this.getFrequentFlyerNumber();
        final Object other$frequentFlyerNumber = other.getFrequentFlyerNumber();
        if (this$frequentFlyerNumber == null ? other$frequentFlyerNumber != null : !this$frequentFlyerNumber.equals(other$frequentFlyerNumber))
            return false;
        final Object this$ticketGds = this.getTicketGds();
        final Object other$ticketGds = other.getTicketGds();
        if (this$ticketGds == null ? other$ticketGds != null : !this$ticketGds.equals(other$ticketGds)) return false;
        final Object this$endorsement = this.getEndorsement();
        final Object other$endorsement = other.getEndorsement();
        if (this$endorsement == null ? other$endorsement != null : !this$endorsement.equals(other$endorsement))
            return false;
        final Object this$grossAmount = this.getGrossAmount();
        final Object other$grossAmount = other.getGrossAmount();
        if (this$grossAmount == null ? other$grossAmount != null : !this$grossAmount.equals(other$grossAmount))
            return false;
        final Object this$netAmount = this.getNetAmount();
        final Object other$netAmount = other.getNetAmount();
        if (this$netAmount == null ? other$netAmount != null : !this$netAmount.equals(other$netAmount)) return false;
        final Object this$standardAgentRate = this.getStandardAgentRate();
        final Object other$standardAgentRate = other.getStandardAgentRate();
        if (this$standardAgentRate == null ? other$standardAgentRate != null : !this$standardAgentRate.equals(other$standardAgentRate))
            return false;
        final Object this$standardAgentFee = this.getStandardAgentFee();
        final Object other$standardAgentFee = other.getStandardAgentFee();
        if (this$standardAgentFee == null ? other$standardAgentFee != null : !this$standardAgentFee.equals(other$standardAgentFee))
            return false;
        final Object this$extraAgentRate = this.getExtraAgentRate();
        final Object other$extraAgentRate = other.getExtraAgentRate();
        if (this$extraAgentRate == null ? other$extraAgentRate != null : !this$extraAgentRate.equals(other$extraAgentRate))
            return false;
        final Object this$extraAgentFee = this.getExtraAgentFee();
        final Object other$extraAgentFee = other.getExtraAgentFee();
        if (this$extraAgentFee == null ? other$extraAgentFee != null : !this$extraAgentFee.equals(other$extraAgentFee))
            return false;
        final Object this$segmentNo1 = this.getSegmentNo1();
        final Object other$segmentNo1 = other.getSegmentNo1();
        if (this$segmentNo1 == null ? other$segmentNo1 != null : !this$segmentNo1.equals(other$segmentNo1))
            return false;
        final Object this$stopover1 = this.getStopover1();
        final Object other$stopover1 = other.getStopover1();
        if (this$stopover1 == null ? other$stopover1 != null : !this$stopover1.equals(other$stopover1)) return false;
        final Object this$mcCarrier1 = this.getMcCarrier1();
        final Object other$mcCarrier1 = other.getMcCarrier1();
        if (this$mcCarrier1 == null ? other$mcCarrier1 != null : !this$mcCarrier1.equals(other$mcCarrier1))
            return false;
        final Object this$mcFlight1 = this.getMcFlight1();
        final Object other$mcFlight1 = other.getMcFlight1();
        if (this$mcFlight1 == null ? other$mcFlight1 != null : !this$mcFlight1.equals(other$mcFlight1)) return false;
        final Object this$ocCarrier1 = this.getOcCarrier1();
        final Object other$ocCarrier1 = other.getOcCarrier1();
        if (this$ocCarrier1 == null ? other$ocCarrier1 != null : !this$ocCarrier1.equals(other$ocCarrier1))
            return false;
        final Object this$ocFlight1 = this.getOcFlight1();
        final Object other$ocFlight1 = other.getOcFlight1();
        if (this$ocFlight1 == null ? other$ocFlight1 != null : !this$ocFlight1.equals(other$ocFlight1)) return false;
        final Object this$saleSegmentType1 = this.getSaleSegmentType1();
        final Object other$saleSegmentType1 = other.getSaleSegmentType1();
        if (this$saleSegmentType1 == null ? other$saleSegmentType1 != null : !this$saleSegmentType1.equals(other$saleSegmentType1))
            return false;
        final Object this$domesticForeignCarrierFlag1 = this.getDomesticForeignCarrierFlag1();
        final Object other$domesticForeignCarrierFlag1 = other.getDomesticForeignCarrierFlag1();
        if (this$domesticForeignCarrierFlag1 == null ? other$domesticForeignCarrierFlag1 != null : !this$domesticForeignCarrierFlag1.equals(other$domesticForeignCarrierFlag1))
            return false;
        final Object this$actualCarrier1 = this.getActualCarrier1();
        final Object other$actualCarrier1 = other.getActualCarrier1();
        if (this$actualCarrier1 == null ? other$actualCarrier1 != null : !this$actualCarrier1.equals(other$actualCarrier1))
            return false;
        final Object this$actualCarrierFlight1 = this.getActualCarrierFlight1();
        final Object other$actualCarrierFlight1 = other.getActualCarrierFlight1();
        if (this$actualCarrierFlight1 == null ? other$actualCarrierFlight1 != null : !this$actualCarrierFlight1.equals(other$actualCarrierFlight1))
            return false;
        final Object this$departureTime1 = this.getDepartureTime1();
        final Object other$departureTime1 = other.getDepartureTime1();
        if (this$departureTime1 == null ? other$departureTime1 != null : !this$departureTime1.equals(other$departureTime1))
            return false;
        final Object this$ticketSegment1 = this.getTicketSegment1();
        final Object other$ticketSegment1 = other.getTicketSegment1();
        if (this$ticketSegment1 == null ? other$ticketSegment1 != null : !this$ticketSegment1.equals(other$ticketSegment1))
            return false;
        final Object this$actualCarrierSegment1 = this.getActualCarrierSegment1();
        final Object other$actualCarrierSegment1 = other.getActualCarrierSegment1();
        if (this$actualCarrierSegment1 == null ? other$actualCarrierSegment1 != null : !this$actualCarrierSegment1.equals(other$actualCarrierSegment1))
            return false;
        final Object this$ticketTravelDate1 = this.getTicketTravelDate1();
        final Object other$ticketTravelDate1 = other.getTicketTravelDate1();
        if (this$ticketTravelDate1 == null ? other$ticketTravelDate1 != null : !this$ticketTravelDate1.equals(other$ticketTravelDate1))
            return false;
        final Object this$transportProcessMonth1 = this.getTransportProcessMonth1();
        final Object other$transportProcessMonth1 = other.getTransportProcessMonth1();
        if (this$transportProcessMonth1 == null ? other$transportProcessMonth1 != null : !this$transportProcessMonth1.equals(other$transportProcessMonth1))
            return false;
        final Object this$actualCarrierDate1 = this.getActualCarrierDate1();
        final Object other$actualCarrierDate1 = other.getActualCarrierDate1();
        if (this$actualCarrierDate1 == null ? other$actualCarrierDate1 != null : !this$actualCarrierDate1.equals(other$actualCarrierDate1))
            return false;
        final Object this$transportSegmentType1 = this.getTransportSegmentType1();
        final Object other$transportSegmentType1 = other.getTransportSegmentType1();
        if (this$transportSegmentType1 == null ? other$transportSegmentType1 != null : !this$transportSegmentType1.equals(other$transportSegmentType1))
            return false;
        final Object this$ticketCabin1 = this.getTicketCabin1();
        final Object other$ticketCabin1 = other.getTicketCabin1();
        if (this$ticketCabin1 == null ? other$ticketCabin1 != null : !this$ticketCabin1.equals(other$ticketCabin1))
            return false;
        final Object this$actualCabin1 = this.getActualCabin1();
        final Object other$actualCabin1 = other.getActualCabin1();
        if (this$actualCabin1 == null ? other$actualCabin1 != null : !this$actualCabin1.equals(other$actualCabin1))
            return false;
        final Object this$saleFba1 = this.getSaleFba1();
        final Object other$saleFba1 = other.getSaleFba1();
        if (this$saleFba1 == null ? other$saleFba1 != null : !this$saleFba1.equals(other$saleFba1)) return false;
        final Object this$transportFba1 = this.getTransportFba1();
        final Object other$transportFba1 = other.getTransportFba1();
        if (this$transportFba1 == null ? other$transportFba1 != null : !this$transportFba1.equals(other$transportFba1))
            return false;
        final Object this$saleApportionment1 = this.getSaleApportionment1();
        final Object other$saleApportionment1 = other.getSaleApportionment1();
        if (this$saleApportionment1 == null ? other$saleApportionment1 != null : !this$saleApportionment1.equals(other$saleApportionment1))
            return false;
        final Object this$saleFuel1 = this.getSaleFuel1();
        final Object other$saleFuel1 = other.getSaleFuel1();
        if (this$saleFuel1 == null ? other$saleFuel1 != null : !this$saleFuel1.equals(other$saleFuel1)) return false;
        final Object this$transportApportionment1 = this.getTransportApportionment1();
        final Object other$transportApportionment1 = other.getTransportApportionment1();
        if (this$transportApportionment1 == null ? other$transportApportionment1 != null : !this$transportApportionment1.equals(other$transportApportionment1))
            return false;
        final Object this$transportFuel1 = this.getTransportFuel1();
        final Object other$transportFuel1 = other.getTransportFuel1();
        if (this$transportFuel1 == null ? other$transportFuel1 != null : !this$transportFuel1.equals(other$transportFuel1))
            return false;
        final Object this$saleCommission1 = this.getSaleCommission1();
        final Object other$saleCommission1 = other.getSaleCommission1();
        if (this$saleCommission1 == null ? other$saleCommission1 != null : !this$saleCommission1.equals(other$saleCommission1))
            return false;
        final Object this$segmentNo2 = this.getSegmentNo2();
        final Object other$segmentNo2 = other.getSegmentNo2();
        if (this$segmentNo2 == null ? other$segmentNo2 != null : !this$segmentNo2.equals(other$segmentNo2))
            return false;
        final Object this$stopover2 = this.getStopover2();
        final Object other$stopover2 = other.getStopover2();
        if (this$stopover2 == null ? other$stopover2 != null : !this$stopover2.equals(other$stopover2)) return false;
        final Object this$mcCarrier2 = this.getMcCarrier2();
        final Object other$mcCarrier2 = other.getMcCarrier2();
        if (this$mcCarrier2 == null ? other$mcCarrier2 != null : !this$mcCarrier2.equals(other$mcCarrier2))
            return false;
        final Object this$mcFlight2 = this.getMcFlight2();
        final Object other$mcFlight2 = other.getMcFlight2();
        if (this$mcFlight2 == null ? other$mcFlight2 != null : !this$mcFlight2.equals(other$mcFlight2)) return false;
        final Object this$ocCarrier2 = this.getOcCarrier2();
        final Object other$ocCarrier2 = other.getOcCarrier2();
        if (this$ocCarrier2 == null ? other$ocCarrier2 != null : !this$ocCarrier2.equals(other$ocCarrier2))
            return false;
        final Object this$ocFlight2 = this.getOcFlight2();
        final Object other$ocFlight2 = other.getOcFlight2();
        if (this$ocFlight2 == null ? other$ocFlight2 != null : !this$ocFlight2.equals(other$ocFlight2)) return false;
        final Object this$saleSegmentType2 = this.getSaleSegmentType2();
        final Object other$saleSegmentType2 = other.getSaleSegmentType2();
        if (this$saleSegmentType2 == null ? other$saleSegmentType2 != null : !this$saleSegmentType2.equals(other$saleSegmentType2))
            return false;
        final Object this$domesticForeignCarrierFlag2 = this.getDomesticForeignCarrierFlag2();
        final Object other$domesticForeignCarrierFlag2 = other.getDomesticForeignCarrierFlag2();
        if (this$domesticForeignCarrierFlag2 == null ? other$domesticForeignCarrierFlag2 != null : !this$domesticForeignCarrierFlag2.equals(other$domesticForeignCarrierFlag2))
            return false;
        final Object this$actualCarrier2 = this.getActualCarrier2();
        final Object other$actualCarrier2 = other.getActualCarrier2();
        if (this$actualCarrier2 == null ? other$actualCarrier2 != null : !this$actualCarrier2.equals(other$actualCarrier2))
            return false;
        final Object this$actualCarrierFlight2 = this.getActualCarrierFlight2();
        final Object other$actualCarrierFlight2 = other.getActualCarrierFlight2();
        if (this$actualCarrierFlight2 == null ? other$actualCarrierFlight2 != null : !this$actualCarrierFlight2.equals(other$actualCarrierFlight2))
            return false;
        final Object this$departureTime2 = this.getDepartureTime2();
        final Object other$departureTime2 = other.getDepartureTime2();
        if (this$departureTime2 == null ? other$departureTime2 != null : !this$departureTime2.equals(other$departureTime2))
            return false;
        final Object this$ticketSegment2 = this.getTicketSegment2();
        final Object other$ticketSegment2 = other.getTicketSegment2();
        if (this$ticketSegment2 == null ? other$ticketSegment2 != null : !this$ticketSegment2.equals(other$ticketSegment2))
            return false;
        final Object this$actualCarrierSegmentCpn2 = this.getActualCarrierSegmentCpn2();
        final Object other$actualCarrierSegmentCpn2 = other.getActualCarrierSegmentCpn2();
        if (this$actualCarrierSegmentCpn2 == null ? other$actualCarrierSegmentCpn2 != null : !this$actualCarrierSegmentCpn2.equals(other$actualCarrierSegmentCpn2))
            return false;
        final Object this$ticketTravelDate2 = this.getTicketTravelDate2();
        final Object other$ticketTravelDate2 = other.getTicketTravelDate2();
        if (this$ticketTravelDate2 == null ? other$ticketTravelDate2 != null : !this$ticketTravelDate2.equals(other$ticketTravelDate2))
            return false;
        final Object this$transportProcessMonth2 = this.getTransportProcessMonth2();
        final Object other$transportProcessMonth2 = other.getTransportProcessMonth2();
        if (this$transportProcessMonth2 == null ? other$transportProcessMonth2 != null : !this$transportProcessMonth2.equals(other$transportProcessMonth2))
            return false;
        final Object this$actualCarrierDateCpn2 = this.getActualCarrierDateCpn2();
        final Object other$actualCarrierDateCpn2 = other.getActualCarrierDateCpn2();
        if (this$actualCarrierDateCpn2 == null ? other$actualCarrierDateCpn2 != null : !this$actualCarrierDateCpn2.equals(other$actualCarrierDateCpn2))
            return false;
        final Object this$transportSegmentTypeCpn2 = this.getTransportSegmentTypeCpn2();
        final Object other$transportSegmentTypeCpn2 = other.getTransportSegmentTypeCpn2();
        if (this$transportSegmentTypeCpn2 == null ? other$transportSegmentTypeCpn2 != null : !this$transportSegmentTypeCpn2.equals(other$transportSegmentTypeCpn2))
            return false;
        final Object this$ticketCabin2 = this.getTicketCabin2();
        final Object other$ticketCabin2 = other.getTicketCabin2();
        if (this$ticketCabin2 == null ? other$ticketCabin2 != null : !this$ticketCabin2.equals(other$ticketCabin2))
            return false;
        final Object this$actualCabinCpn2 = this.getActualCabinCpn2();
        final Object other$actualCabinCpn2 = other.getActualCabinCpn2();
        if (this$actualCabinCpn2 == null ? other$actualCabinCpn2 != null : !this$actualCabinCpn2.equals(other$actualCabinCpn2))
            return false;
        final Object this$saleFba2 = this.getSaleFba2();
        final Object other$saleFba2 = other.getSaleFba2();
        if (this$saleFba2 == null ? other$saleFba2 != null : !this$saleFba2.equals(other$saleFba2)) return false;
        final Object this$transportFba2 = this.getTransportFba2();
        final Object other$transportFba2 = other.getTransportFba2();
        if (this$transportFba2 == null ? other$transportFba2 != null : !this$transportFba2.equals(other$transportFba2))
            return false;
        final Object this$saleApportionment2 = this.getSaleApportionment2();
        final Object other$saleApportionment2 = other.getSaleApportionment2();
        if (this$saleApportionment2 == null ? other$saleApportionment2 != null : !this$saleApportionment2.equals(other$saleApportionment2))
            return false;
        final Object this$saleFuel2 = this.getSaleFuel2();
        final Object other$saleFuel2 = other.getSaleFuel2();
        if (this$saleFuel2 == null ? other$saleFuel2 != null : !this$saleFuel2.equals(other$saleFuel2)) return false;
        final Object this$transportApportionment2 = this.getTransportApportionment2();
        final Object other$transportApportionment2 = other.getTransportApportionment2();
        if (this$transportApportionment2 == null ? other$transportApportionment2 != null : !this$transportApportionment2.equals(other$transportApportionment2))
            return false;
        final Object this$transportFuel2 = this.getTransportFuel2();
        final Object other$transportFuel2 = other.getTransportFuel2();
        if (this$transportFuel2 == null ? other$transportFuel2 != null : !this$transportFuel2.equals(other$transportFuel2))
            return false;
        final Object this$saleCommission2 = this.getSaleCommission2();
        final Object other$saleCommission2 = other.getSaleCommission2();
        if (this$saleCommission2 == null ? other$saleCommission2 != null : !this$saleCommission2.equals(other$saleCommission2))
            return false;
        final Object this$segmentNo3 = this.getSegmentNo3();
        final Object other$segmentNo3 = other.getSegmentNo3();
        if (this$segmentNo3 == null ? other$segmentNo3 != null : !this$segmentNo3.equals(other$segmentNo3))
            return false;
        final Object this$stopover3 = this.getStopover3();
        final Object other$stopover3 = other.getStopover3();
        if (this$stopover3 == null ? other$stopover3 != null : !this$stopover3.equals(other$stopover3)) return false;
        final Object this$mcCarrier3 = this.getMcCarrier3();
        final Object other$mcCarrier3 = other.getMcCarrier3();
        if (this$mcCarrier3 == null ? other$mcCarrier3 != null : !this$mcCarrier3.equals(other$mcCarrier3))
            return false;
        final Object this$mcFlight3 = this.getMcFlight3();
        final Object other$mcFlight3 = other.getMcFlight3();
        if (this$mcFlight3 == null ? other$mcFlight3 != null : !this$mcFlight3.equals(other$mcFlight3)) return false;
        final Object this$ocCarrier3 = this.getOcCarrier3();
        final Object other$ocCarrier3 = other.getOcCarrier3();
        if (this$ocCarrier3 == null ? other$ocCarrier3 != null : !this$ocCarrier3.equals(other$ocCarrier3))
            return false;
        final Object this$ocFlight3 = this.getOcFlight3();
        final Object other$ocFlight3 = other.getOcFlight3();
        if (this$ocFlight3 == null ? other$ocFlight3 != null : !this$ocFlight3.equals(other$ocFlight3)) return false;
        final Object this$saleSegmentType3 = this.getSaleSegmentType3();
        final Object other$saleSegmentType3 = other.getSaleSegmentType3();
        if (this$saleSegmentType3 == null ? other$saleSegmentType3 != null : !this$saleSegmentType3.equals(other$saleSegmentType3))
            return false;
        final Object this$domesticForeignCarrierFlag3 = this.getDomesticForeignCarrierFlag3();
        final Object other$domesticForeignCarrierFlag3 = other.getDomesticForeignCarrierFlag3();
        if (this$domesticForeignCarrierFlag3 == null ? other$domesticForeignCarrierFlag3 != null : !this$domesticForeignCarrierFlag3.equals(other$domesticForeignCarrierFlag3))
            return false;
        final Object this$actualCarrier3 = this.getActualCarrier3();
        final Object other$actualCarrier3 = other.getActualCarrier3();
        if (this$actualCarrier3 == null ? other$actualCarrier3 != null : !this$actualCarrier3.equals(other$actualCarrier3))
            return false;
        final Object this$actualCarrierFlight3 = this.getActualCarrierFlight3();
        final Object other$actualCarrierFlight3 = other.getActualCarrierFlight3();
        if (this$actualCarrierFlight3 == null ? other$actualCarrierFlight3 != null : !this$actualCarrierFlight3.equals(other$actualCarrierFlight3))
            return false;
        final Object this$departureTimeCpn3 = this.getDepartureTimeCpn3();
        final Object other$departureTimeCpn3 = other.getDepartureTimeCpn3();
        if (this$departureTimeCpn3 == null ? other$departureTimeCpn3 != null : !this$departureTimeCpn3.equals(other$departureTimeCpn3))
            return false;
        final Object this$ticketSegmentCpn3 = this.getTicketSegmentCpn3();
        final Object other$ticketSegmentCpn3 = other.getTicketSegmentCpn3();
        if (this$ticketSegmentCpn3 == null ? other$ticketSegmentCpn3 != null : !this$ticketSegmentCpn3.equals(other$ticketSegmentCpn3))
            return false;
        final Object this$actualCarrierSegmentCpn3 = this.getActualCarrierSegmentCpn3();
        final Object other$actualCarrierSegmentCpn3 = other.getActualCarrierSegmentCpn3();
        if (this$actualCarrierSegmentCpn3 == null ? other$actualCarrierSegmentCpn3 != null : !this$actualCarrierSegmentCpn3.equals(other$actualCarrierSegmentCpn3))
            return false;
        final Object this$ticketTravelDate3 = this.getTicketTravelDate3();
        final Object other$ticketTravelDate3 = other.getTicketTravelDate3();
        if (this$ticketTravelDate3 == null ? other$ticketTravelDate3 != null : !this$ticketTravelDate3.equals(other$ticketTravelDate3))
            return false;
        final Object this$transportProcessMonth3 = this.getTransportProcessMonth3();
        final Object other$transportProcessMonth3 = other.getTransportProcessMonth3();
        if (this$transportProcessMonth3 == null ? other$transportProcessMonth3 != null : !this$transportProcessMonth3.equals(other$transportProcessMonth3))
            return false;
        final Object this$actualCarrierDate3 = this.getActualCarrierDate3();
        final Object other$actualCarrierDate3 = other.getActualCarrierDate3();
        if (this$actualCarrierDate3 == null ? other$actualCarrierDate3 != null : !this$actualCarrierDate3.equals(other$actualCarrierDate3))
            return false;
        final Object this$transportSegmentType3 = this.getTransportSegmentType3();
        final Object other$transportSegmentType3 = other.getTransportSegmentType3();
        if (this$transportSegmentType3 == null ? other$transportSegmentType3 != null : !this$transportSegmentType3.equals(other$transportSegmentType3))
            return false;
        final Object this$ticketCabin3 = this.getTicketCabin3();
        final Object other$ticketCabin3 = other.getTicketCabin3();
        if (this$ticketCabin3 == null ? other$ticketCabin3 != null : !this$ticketCabin3.equals(other$ticketCabin3))
            return false;
        final Object this$actualCabin3 = this.getActualCabin3();
        final Object other$actualCabin3 = other.getActualCabin3();
        if (this$actualCabin3 == null ? other$actualCabin3 != null : !this$actualCabin3.equals(other$actualCabin3))
            return false;
        final Object this$saleFba3 = this.getSaleFba3();
        final Object other$saleFba3 = other.getSaleFba3();
        if (this$saleFba3 == null ? other$saleFba3 != null : !this$saleFba3.equals(other$saleFba3)) return false;
        final Object this$transportFba3 = this.getTransportFba3();
        final Object other$transportFba3 = other.getTransportFba3();
        if (this$transportFba3 == null ? other$transportFba3 != null : !this$transportFba3.equals(other$transportFba3))
            return false;
        final Object this$saleApportionment3 = this.getSaleApportionment3();
        final Object other$saleApportionment3 = other.getSaleApportionment3();
        if (this$saleApportionment3 == null ? other$saleApportionment3 != null : !this$saleApportionment3.equals(other$saleApportionment3))
            return false;
        final Object this$saleFuel3 = this.getSaleFuel3();
        final Object other$saleFuel3 = other.getSaleFuel3();
        if (this$saleFuel3 == null ? other$saleFuel3 != null : !this$saleFuel3.equals(other$saleFuel3)) return false;
        final Object this$transportApportionment3 = this.getTransportApportionment3();
        final Object other$transportApportionment3 = other.getTransportApportionment3();
        if (this$transportApportionment3 == null ? other$transportApportionment3 != null : !this$transportApportionment3.equals(other$transportApportionment3))
            return false;
        final Object this$transportFuel3 = this.getTransportFuel3();
        final Object other$transportFuel3 = other.getTransportFuel3();
        if (this$transportFuel3 == null ? other$transportFuel3 != null : !this$transportFuel3.equals(other$transportFuel3))
            return false;
        final Object this$saleCommission3 = this.getSaleCommission3();
        final Object other$saleCommission3 = other.getSaleCommission3();
        if (this$saleCommission3 == null ? other$saleCommission3 != null : !this$saleCommission3.equals(other$saleCommission3))
            return false;
        final Object this$segmentNo4 = this.getSegmentNo4();
        final Object other$segmentNo4 = other.getSegmentNo4();
        if (this$segmentNo4 == null ? other$segmentNo4 != null : !this$segmentNo4.equals(other$segmentNo4))
            return false;
        final Object this$stopover4 = this.getStopover4();
        final Object other$stopover4 = other.getStopover4();
        if (this$stopover4 == null ? other$stopover4 != null : !this$stopover4.equals(other$stopover4)) return false;
        final Object this$mcCarrier4 = this.getMcCarrier4();
        final Object other$mcCarrier4 = other.getMcCarrier4();
        if (this$mcCarrier4 == null ? other$mcCarrier4 != null : !this$mcCarrier4.equals(other$mcCarrier4))
            return false;
        final Object this$mcFlight4 = this.getMcFlight4();
        final Object other$mcFlight4 = other.getMcFlight4();
        if (this$mcFlight4 == null ? other$mcFlight4 != null : !this$mcFlight4.equals(other$mcFlight4)) return false;
        final Object this$ocCarrier4 = this.getOcCarrier4();
        final Object other$ocCarrier4 = other.getOcCarrier4();
        if (this$ocCarrier4 == null ? other$ocCarrier4 != null : !this$ocCarrier4.equals(other$ocCarrier4))
            return false;
        final Object this$ocFlight4 = this.getOcFlight4();
        final Object other$ocFlight4 = other.getOcFlight4();
        if (this$ocFlight4 == null ? other$ocFlight4 != null : !this$ocFlight4.equals(other$ocFlight4)) return false;
        final Object this$saleSegmentType4 = this.getSaleSegmentType4();
        final Object other$saleSegmentType4 = other.getSaleSegmentType4();
        if (this$saleSegmentType4 == null ? other$saleSegmentType4 != null : !this$saleSegmentType4.equals(other$saleSegmentType4))
            return false;
        final Object this$domesticForeignCarrierFlag4 = this.getDomesticForeignCarrierFlag4();
        final Object other$domesticForeignCarrierFlag4 = other.getDomesticForeignCarrierFlag4();
        if (this$domesticForeignCarrierFlag4 == null ? other$domesticForeignCarrierFlag4 != null : !this$domesticForeignCarrierFlag4.equals(other$domesticForeignCarrierFlag4))
            return false;
        final Object this$actualCarrier4 = this.getActualCarrier4();
        final Object other$actualCarrier4 = other.getActualCarrier4();
        if (this$actualCarrier4 == null ? other$actualCarrier4 != null : !this$actualCarrier4.equals(other$actualCarrier4))
            return false;
        final Object this$actualCarrierFlight4 = this.getActualCarrierFlight4();
        final Object other$actualCarrierFlight4 = other.getActualCarrierFlight4();
        if (this$actualCarrierFlight4 == null ? other$actualCarrierFlight4 != null : !this$actualCarrierFlight4.equals(other$actualCarrierFlight4))
            return false;
        final Object this$departureTimeCpn4 = this.getDepartureTimeCpn4();
        final Object other$departureTimeCpn4 = other.getDepartureTimeCpn4();
        if (this$departureTimeCpn4 == null ? other$departureTimeCpn4 != null : !this$departureTimeCpn4.equals(other$departureTimeCpn4))
            return false;
        final Object this$ticketSegment4 = this.getTicketSegment4();
        final Object other$ticketSegment4 = other.getTicketSegment4();
        if (this$ticketSegment4 == null ? other$ticketSegment4 != null : !this$ticketSegment4.equals(other$ticketSegment4))
            return false;
        final Object this$actualCarrierSegment4 = this.getActualCarrierSegment4();
        final Object other$actualCarrierSegment4 = other.getActualCarrierSegment4();
        if (this$actualCarrierSegment4 == null ? other$actualCarrierSegment4 != null : !this$actualCarrierSegment4.equals(other$actualCarrierSegment4))
            return false;
        final Object this$ticketTravelDate4 = this.getTicketTravelDate4();
        final Object other$ticketTravelDate4 = other.getTicketTravelDate4();
        if (this$ticketTravelDate4 == null ? other$ticketTravelDate4 != null : !this$ticketTravelDate4.equals(other$ticketTravelDate4))
            return false;
        final Object this$transportProcessMonth4 = this.getTransportProcessMonth4();
        final Object other$transportProcessMonth4 = other.getTransportProcessMonth4();
        if (this$transportProcessMonth4 == null ? other$transportProcessMonth4 != null : !this$transportProcessMonth4.equals(other$transportProcessMonth4))
            return false;
        final Object this$actualCarrierDate4 = this.getActualCarrierDate4();
        final Object other$actualCarrierDate4 = other.getActualCarrierDate4();
        if (this$actualCarrierDate4 == null ? other$actualCarrierDate4 != null : !this$actualCarrierDate4.equals(other$actualCarrierDate4))
            return false;
        final Object this$transportSegmentType4 = this.getTransportSegmentType4();
        final Object other$transportSegmentType4 = other.getTransportSegmentType4();
        if (this$transportSegmentType4 == null ? other$transportSegmentType4 != null : !this$transportSegmentType4.equals(other$transportSegmentType4))
            return false;
        final Object this$ticketCabin4 = this.getTicketCabin4();
        final Object other$ticketCabin4 = other.getTicketCabin4();
        if (this$ticketCabin4 == null ? other$ticketCabin4 != null : !this$ticketCabin4.equals(other$ticketCabin4))
            return false;
        final Object this$actualCabin4 = this.getActualCabin4();
        final Object other$actualCabin4 = other.getActualCabin4();
        if (this$actualCabin4 == null ? other$actualCabin4 != null : !this$actualCabin4.equals(other$actualCabin4))
            return false;
        final Object this$saleFba4 = this.getSaleFba4();
        final Object other$saleFba4 = other.getSaleFba4();
        if (this$saleFba4 == null ? other$saleFba4 != null : !this$saleFba4.equals(other$saleFba4)) return false;
        final Object this$transportFba4 = this.getTransportFba4();
        final Object other$transportFba4 = other.getTransportFba4();
        if (this$transportFba4 == null ? other$transportFba4 != null : !this$transportFba4.equals(other$transportFba4))
            return false;
        final Object this$saleApportionment4 = this.getSaleApportionment4();
        final Object other$saleApportionment4 = other.getSaleApportionment4();
        if (this$saleApportionment4 == null ? other$saleApportionment4 != null : !this$saleApportionment4.equals(other$saleApportionment4))
            return false;
        final Object this$saleFuel4 = this.getSaleFuel4();
        final Object other$saleFuel4 = other.getSaleFuel4();
        if (this$saleFuel4 == null ? other$saleFuel4 != null : !this$saleFuel4.equals(other$saleFuel4)) return false;
        final Object this$transportApportionment4 = this.getTransportApportionment4();
        final Object other$transportApportionment4 = other.getTransportApportionment4();
        if (this$transportApportionment4 == null ? other$transportApportionment4 != null : !this$transportApportionment4.equals(other$transportApportionment4))
            return false;
        final Object this$transportFuel4 = this.getTransportFuel4();
        final Object other$transportFuel4 = other.getTransportFuel4();
        if (this$transportFuel4 == null ? other$transportFuel4 != null : !this$transportFuel4.equals(other$transportFuel4))
            return false;
        final Object this$saleCommission4 = this.getSaleCommission4();
        final Object other$saleCommission4 = other.getSaleCommission4();
        if (this$saleCommission4 == null ? other$saleCommission4 != null : !this$saleCommission4.equals(other$saleCommission4))
            return false;
        final Object this$etlCreateTime = this.getEtlCreateTime();
        final Object other$etlCreateTime = other.getEtlCreateTime();
        if (this$etlCreateTime == null ? other$etlCreateTime != null : !this$etlCreateTime.equals(other$etlCreateTime))
            return false;
        final Object this$etlUpdateTime = this.getEtlUpdateTime();
        final Object other$etlUpdateTime = other.getEtlUpdateTime();
        if (this$etlUpdateTime == null ? other$etlUpdateTime != null : !this$etlUpdateTime.equals(other$etlUpdateTime))
            return false;
        final Object this$etlDate = this.getEtlDate();
        final Object other$etlDate = other.getEtlDate();
        if (this$etlDate == null ? other$etlDate != null : !this$etlDate.equals(other$etlDate)) return false;
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof TOdsYxwSaleReport;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final Object $sdasrc = this.getSdasrc();
        result = result * PRIME + ($sdasrc == null ? 43 : $sdasrc.hashCode());
        final Object $sdasta = this.getSdasta();
        result = result * PRIME + ($sdasta == null ? 43 : $sdasta.hashCode());
        final Object $electronicTicketType = this.getElectronicTicketType();
        result = result * PRIME + ($electronicTicketType == null ? 43 : $electronicTicketType.hashCode());
        final Object $agentCode = this.getAgentCode();
        result = result * PRIME + ($agentCode == null ? 43 : $agentCode.hashCode());
        final Object $agentName = this.getAgentName();
        result = result * PRIME + ($agentName == null ? 43 : $agentName.hashCode());
        final Object $saleType = this.getSaleType();
        result = result * PRIME + ($saleType == null ? 43 : $saleType.hashCode());
        final Object $originalTicketNumber = this.getOriginalTicketNumber();
        result = result * PRIME + ($originalTicketNumber == null ? 43 : $originalTicketNumber.hashCode());
        final Object $saleProcessMonth = this.getSaleProcessMonth();
        result = result * PRIME + ($saleProcessMonth == null ? 43 : $saleProcessMonth.hashCode());
        final Object $saleDate = this.getSaleDate();
        result = result * PRIME + ($saleDate == null ? 43 : $saleDate.hashCode());
        final Object $ticketNumber = this.getTicketNumber();
        result = result * PRIME + ($ticketNumber == null ? 43 : $ticketNumber.hashCode());
        final Object $ticketRoute = this.getTicketRoute();
        result = result * PRIME + ($ticketRoute == null ? 43 : $ticketRoute.hashCode());
        final Object $routeType = this.getRouteType();
        result = result * PRIME + ($routeType == null ? 43 : $routeType.hashCode());
        final Object $tourCode = this.getTourCode();
        result = result * PRIME + ($tourCode == null ? 43 : $tourCode.hashCode());
        final Object $foreignCarrierFlag = this.getForeignCarrierFlag();
        result = result * PRIME + ($foreignCarrierFlag == null ? 43 : $foreignCarrierFlag.hashCode());
        final Object $groupIndividualFlag = this.getGroupIndividualFlag();
        result = result * PRIME + ($groupIndividualFlag == null ? 43 : $groupIndividualFlag.hashCode());
        final Object $teamName = this.getTeamName();
        result = result * PRIME + ($teamName == null ? 43 : $teamName.hashCode());
        final Object $passengerType = this.getPassengerType();
        result = result * PRIME + ($passengerType == null ? 43 : $passengerType.hashCode());
        final Object $linkedTicketFlag = this.getLinkedTicketFlag();
        result = result * PRIME + ($linkedTicketFlag == null ? 43 : $linkedTicketFlag.hashCode());
        final Object $frequentFlyerNumber = this.getFrequentFlyerNumber();
        result = result * PRIME + ($frequentFlyerNumber == null ? 43 : $frequentFlyerNumber.hashCode());
        final Object $ticketGds = this.getTicketGds();
        result = result * PRIME + ($ticketGds == null ? 43 : $ticketGds.hashCode());
        final Object $endorsement = this.getEndorsement();
        result = result * PRIME + ($endorsement == null ? 43 : $endorsement.hashCode());
        final Object $grossAmount = this.getGrossAmount();
        result = result * PRIME + ($grossAmount == null ? 43 : $grossAmount.hashCode());
        final Object $netAmount = this.getNetAmount();
        result = result * PRIME + ($netAmount == null ? 43 : $netAmount.hashCode());
        final Object $standardAgentRate = this.getStandardAgentRate();
        result = result * PRIME + ($standardAgentRate == null ? 43 : $standardAgentRate.hashCode());
        final Object $standardAgentFee = this.getStandardAgentFee();
        result = result * PRIME + ($standardAgentFee == null ? 43 : $standardAgentFee.hashCode());
        final Object $extraAgentRate = this.getExtraAgentRate();
        result = result * PRIME + ($extraAgentRate == null ? 43 : $extraAgentRate.hashCode());
        final Object $extraAgentFee = this.getExtraAgentFee();
        result = result * PRIME + ($extraAgentFee == null ? 43 : $extraAgentFee.hashCode());
        final Object $segmentNo1 = this.getSegmentNo1();
        result = result * PRIME + ($segmentNo1 == null ? 43 : $segmentNo1.hashCode());
        final Object $stopover1 = this.getStopover1();
        result = result * PRIME + ($stopover1 == null ? 43 : $stopover1.hashCode());
        final Object $mcCarrier1 = this.getMcCarrier1();
        result = result * PRIME + ($mcCarrier1 == null ? 43 : $mcCarrier1.hashCode());
        final Object $mcFlight1 = this.getMcFlight1();
        result = result * PRIME + ($mcFlight1 == null ? 43 : $mcFlight1.hashCode());
        final Object $ocCarrier1 = this.getOcCarrier1();
        result = result * PRIME + ($ocCarrier1 == null ? 43 : $ocCarrier1.hashCode());
        final Object $ocFlight1 = this.getOcFlight1();
        result = result * PRIME + ($ocFlight1 == null ? 43 : $ocFlight1.hashCode());
        final Object $saleSegmentType1 = this.getSaleSegmentType1();
        result = result * PRIME + ($saleSegmentType1 == null ? 43 : $saleSegmentType1.hashCode());
        final Object $domesticForeignCarrierFlag1 = this.getDomesticForeignCarrierFlag1();
        result = result * PRIME + ($domesticForeignCarrierFlag1 == null ? 43 : $domesticForeignCarrierFlag1.hashCode());
        final Object $actualCarrier1 = this.getActualCarrier1();
        result = result * PRIME + ($actualCarrier1 == null ? 43 : $actualCarrier1.hashCode());
        final Object $actualCarrierFlight1 = this.getActualCarrierFlight1();
        result = result * PRIME + ($actualCarrierFlight1 == null ? 43 : $actualCarrierFlight1.hashCode());
        final Object $departureTime1 = this.getDepartureTime1();
        result = result * PRIME + ($departureTime1 == null ? 43 : $departureTime1.hashCode());
        final Object $ticketSegment1 = this.getTicketSegment1();
        result = result * PRIME + ($ticketSegment1 == null ? 43 : $ticketSegment1.hashCode());
        final Object $actualCarrierSegment1 = this.getActualCarrierSegment1();
        result = result * PRIME + ($actualCarrierSegment1 == null ? 43 : $actualCarrierSegment1.hashCode());
        final Object $ticketTravelDate1 = this.getTicketTravelDate1();
        result = result * PRIME + ($ticketTravelDate1 == null ? 43 : $ticketTravelDate1.hashCode());
        final Object $transportProcessMonth1 = this.getTransportProcessMonth1();
        result = result * PRIME + ($transportProcessMonth1 == null ? 43 : $transportProcessMonth1.hashCode());
        final Object $actualCarrierDate1 = this.getActualCarrierDate1();
        result = result * PRIME + ($actualCarrierDate1 == null ? 43 : $actualCarrierDate1.hashCode());
        final Object $transportSegmentType1 = this.getTransportSegmentType1();
        result = result * PRIME + ($transportSegmentType1 == null ? 43 : $transportSegmentType1.hashCode());
        final Object $ticketCabin1 = this.getTicketCabin1();
        result = result * PRIME + ($ticketCabin1 == null ? 43 : $ticketCabin1.hashCode());
        final Object $actualCabin1 = this.getActualCabin1();
        result = result * PRIME + ($actualCabin1 == null ? 43 : $actualCabin1.hashCode());
        final Object $saleFba1 = this.getSaleFba1();
        result = result * PRIME + ($saleFba1 == null ? 43 : $saleFba1.hashCode());
        final Object $transportFba1 = this.getTransportFba1();
        result = result * PRIME + ($transportFba1 == null ? 43 : $transportFba1.hashCode());
        final Object $saleApportionment1 = this.getSaleApportionment1();
        result = result * PRIME + ($saleApportionment1 == null ? 43 : $saleApportionment1.hashCode());
        final Object $saleFuel1 = this.getSaleFuel1();
        result = result * PRIME + ($saleFuel1 == null ? 43 : $saleFuel1.hashCode());
        final Object $transportApportionment1 = this.getTransportApportionment1();
        result = result * PRIME + ($transportApportionment1 == null ? 43 : $transportApportionment1.hashCode());
        final Object $transportFuel1 = this.getTransportFuel1();
        result = result * PRIME + ($transportFuel1 == null ? 43 : $transportFuel1.hashCode());
        final Object $saleCommission1 = this.getSaleCommission1();
        result = result * PRIME + ($saleCommission1 == null ? 43 : $saleCommission1.hashCode());
        final Object $segmentNo2 = this.getSegmentNo2();
        result = result * PRIME + ($segmentNo2 == null ? 43 : $segmentNo2.hashCode());
        final Object $stopover2 = this.getStopover2();
        result = result * PRIME + ($stopover2 == null ? 43 : $stopover2.hashCode());
        final Object $mcCarrier2 = this.getMcCarrier2();
        result = result * PRIME + ($mcCarrier2 == null ? 43 : $mcCarrier2.hashCode());
        final Object $mcFlight2 = this.getMcFlight2();
        result = result * PRIME + ($mcFlight2 == null ? 43 : $mcFlight2.hashCode());
        final Object $ocCarrier2 = this.getOcCarrier2();
        result = result * PRIME + ($ocCarrier2 == null ? 43 : $ocCarrier2.hashCode());
        final Object $ocFlight2 = this.getOcFlight2();
        result = result * PRIME + ($ocFlight2 == null ? 43 : $ocFlight2.hashCode());
        final Object $saleSegmentType2 = this.getSaleSegmentType2();
        result = result * PRIME + ($saleSegmentType2 == null ? 43 : $saleSegmentType2.hashCode());
        final Object $domesticForeignCarrierFlag2 = this.getDomesticForeignCarrierFlag2();
        result = result * PRIME + ($domesticForeignCarrierFlag2 == null ? 43 : $domesticForeignCarrierFlag2.hashCode());
        final Object $actualCarrier2 = this.getActualCarrier2();
        result = result * PRIME + ($actualCarrier2 == null ? 43 : $actualCarrier2.hashCode());
        final Object $actualCarrierFlight2 = this.getActualCarrierFlight2();
        result = result * PRIME + ($actualCarrierFlight2 == null ? 43 : $actualCarrierFlight2.hashCode());
        final Object $departureTime2 = this.getDepartureTime2();
        result = result * PRIME + ($departureTime2 == null ? 43 : $departureTime2.hashCode());
        final Object $ticketSegment2 = this.getTicketSegment2();
        result = result * PRIME + ($ticketSegment2 == null ? 43 : $ticketSegment2.hashCode());
        final Object $actualCarrierSegmentCpn2 = this.getActualCarrierSegmentCpn2();
        result = result * PRIME + ($actualCarrierSegmentCpn2 == null ? 43 : $actualCarrierSegmentCpn2.hashCode());
        final Object $ticketTravelDate2 = this.getTicketTravelDate2();
        result = result * PRIME + ($ticketTravelDate2 == null ? 43 : $ticketTravelDate2.hashCode());
        final Object $transportProcessMonth2 = this.getTransportProcessMonth2();
        result = result * PRIME + ($transportProcessMonth2 == null ? 43 : $transportProcessMonth2.hashCode());
        final Object $actualCarrierDateCpn2 = this.getActualCarrierDateCpn2();
        result = result * PRIME + ($actualCarrierDateCpn2 == null ? 43 : $actualCarrierDateCpn2.hashCode());
        final Object $transportSegmentTypeCpn2 = this.getTransportSegmentTypeCpn2();
        result = result * PRIME + ($transportSegmentTypeCpn2 == null ? 43 : $transportSegmentTypeCpn2.hashCode());
        final Object $ticketCabin2 = this.getTicketCabin2();
        result = result * PRIME + ($ticketCabin2 == null ? 43 : $ticketCabin2.hashCode());
        final Object $actualCabinCpn2 = this.getActualCabinCpn2();
        result = result * PRIME + ($actualCabinCpn2 == null ? 43 : $actualCabinCpn2.hashCode());
        final Object $saleFba2 = this.getSaleFba2();
        result = result * PRIME + ($saleFba2 == null ? 43 : $saleFba2.hashCode());
        final Object $transportFba2 = this.getTransportFba2();
        result = result * PRIME + ($transportFba2 == null ? 43 : $transportFba2.hashCode());
        final Object $saleApportionment2 = this.getSaleApportionment2();
        result = result * PRIME + ($saleApportionment2 == null ? 43 : $saleApportionment2.hashCode());
        final Object $saleFuel2 = this.getSaleFuel2();
        result = result * PRIME + ($saleFuel2 == null ? 43 : $saleFuel2.hashCode());
        final Object $transportApportionment2 = this.getTransportApportionment2();
        result = result * PRIME + ($transportApportionment2 == null ? 43 : $transportApportionment2.hashCode());
        final Object $transportFuel2 = this.getTransportFuel2();
        result = result * PRIME + ($transportFuel2 == null ? 43 : $transportFuel2.hashCode());
        final Object $saleCommission2 = this.getSaleCommission2();
        result = result * PRIME + ($saleCommission2 == null ? 43 : $saleCommission2.hashCode());
        final Object $segmentNo3 = this.getSegmentNo3();
        result = result * PRIME + ($segmentNo3 == null ? 43 : $segmentNo3.hashCode());
        final Object $stopover3 = this.getStopover3();
        result = result * PRIME + ($stopover3 == null ? 43 : $stopover3.hashCode());
        final Object $mcCarrier3 = this.getMcCarrier3();
        result = result * PRIME + ($mcCarrier3 == null ? 43 : $mcCarrier3.hashCode());
        final Object $mcFlight3 = this.getMcFlight3();
        result = result * PRIME + ($mcFlight3 == null ? 43 : $mcFlight3.hashCode());
        final Object $ocCarrier3 = this.getOcCarrier3();
        result = result * PRIME + ($ocCarrier3 == null ? 43 : $ocCarrier3.hashCode());
        final Object $ocFlight3 = this.getOcFlight3();
        result = result * PRIME + ($ocFlight3 == null ? 43 : $ocFlight3.hashCode());
        final Object $saleSegmentType3 = this.getSaleSegmentType3();
        result = result * PRIME + ($saleSegmentType3 == null ? 43 : $saleSegmentType3.hashCode());
        final Object $domesticForeignCarrierFlag3 = this.getDomesticForeignCarrierFlag3();
        result = result * PRIME + ($domesticForeignCarrierFlag3 == null ? 43 : $domesticForeignCarrierFlag3.hashCode());
        final Object $actualCarrier3 = this.getActualCarrier3();
        result = result * PRIME + ($actualCarrier3 == null ? 43 : $actualCarrier3.hashCode());
        final Object $actualCarrierFlight3 = this.getActualCarrierFlight3();
        result = result * PRIME + ($actualCarrierFlight3 == null ? 43 : $actualCarrierFlight3.hashCode());
        final Object $departureTimeCpn3 = this.getDepartureTimeCpn3();
        result = result * PRIME + ($departureTimeCpn3 == null ? 43 : $departureTimeCpn3.hashCode());
        final Object $ticketSegmentCpn3 = this.getTicketSegmentCpn3();
        result = result * PRIME + ($ticketSegmentCpn3 == null ? 43 : $ticketSegmentCpn3.hashCode());
        final Object $actualCarrierSegmentCpn3 = this.getActualCarrierSegmentCpn3();
        result = result * PRIME + ($actualCarrierSegmentCpn3 == null ? 43 : $actualCarrierSegmentCpn3.hashCode());
        final Object $ticketTravelDate3 = this.getTicketTravelDate3();
        result = result * PRIME + ($ticketTravelDate3 == null ? 43 : $ticketTravelDate3.hashCode());
        final Object $transportProcessMonth3 = this.getTransportProcessMonth3();
        result = result * PRIME + ($transportProcessMonth3 == null ? 43 : $transportProcessMonth3.hashCode());
        final Object $actualCarrierDate3 = this.getActualCarrierDate3();
        result = result * PRIME + ($actualCarrierDate3 == null ? 43 : $actualCarrierDate3.hashCode());
        final Object $transportSegmentType3 = this.getTransportSegmentType3();
        result = result * PRIME + ($transportSegmentType3 == null ? 43 : $transportSegmentType3.hashCode());
        final Object $ticketCabin3 = this.getTicketCabin3();
        result = result * PRIME + ($ticketCabin3 == null ? 43 : $ticketCabin3.hashCode());
        final Object $actualCabin3 = this.getActualCabin3();
        result = result * PRIME + ($actualCabin3 == null ? 43 : $actualCabin3.hashCode());
        final Object $saleFba3 = this.getSaleFba3();
        result = result * PRIME + ($saleFba3 == null ? 43 : $saleFba3.hashCode());
        final Object $transportFba3 = this.getTransportFba3();
        result = result * PRIME + ($transportFba3 == null ? 43 : $transportFba3.hashCode());
        final Object $saleApportionment3 = this.getSaleApportionment3();
        result = result * PRIME + ($saleApportionment3 == null ? 43 : $saleApportionment3.hashCode());
        final Object $saleFuel3 = this.getSaleFuel3();
        result = result * PRIME + ($saleFuel3 == null ? 43 : $saleFuel3.hashCode());
        final Object $transportApportionment3 = this.getTransportApportionment3();
        result = result * PRIME + ($transportApportionment3 == null ? 43 : $transportApportionment3.hashCode());
        final Object $transportFuel3 = this.getTransportFuel3();
        result = result * PRIME + ($transportFuel3 == null ? 43 : $transportFuel3.hashCode());
        final Object $saleCommission3 = this.getSaleCommission3();
        result = result * PRIME + ($saleCommission3 == null ? 43 : $saleCommission3.hashCode());
        final Object $segmentNo4 = this.getSegmentNo4();
        result = result * PRIME + ($segmentNo4 == null ? 43 : $segmentNo4.hashCode());
        final Object $stopover4 = this.getStopover4();
        result = result * PRIME + ($stopover4 == null ? 43 : $stopover4.hashCode());
        final Object $mcCarrier4 = this.getMcCarrier4();
        result = result * PRIME + ($mcCarrier4 == null ? 43 : $mcCarrier4.hashCode());
        final Object $mcFlight4 = this.getMcFlight4();
        result = result * PRIME + ($mcFlight4 == null ? 43 : $mcFlight4.hashCode());
        final Object $ocCarrier4 = this.getOcCarrier4();
        result = result * PRIME + ($ocCarrier4 == null ? 43 : $ocCarrier4.hashCode());
        final Object $ocFlight4 = this.getOcFlight4();
        result = result * PRIME + ($ocFlight4 == null ? 43 : $ocFlight4.hashCode());
        final Object $saleSegmentType4 = this.getSaleSegmentType4();
        result = result * PRIME + ($saleSegmentType4 == null ? 43 : $saleSegmentType4.hashCode());
        final Object $domesticForeignCarrierFlag4 = this.getDomesticForeignCarrierFlag4();
        result = result * PRIME + ($domesticForeignCarrierFlag4 == null ? 43 : $domesticForeignCarrierFlag4.hashCode());
        final Object $actualCarrier4 = this.getActualCarrier4();
        result = result * PRIME + ($actualCarrier4 == null ? 43 : $actualCarrier4.hashCode());
        final Object $actualCarrierFlight4 = this.getActualCarrierFlight4();
        result = result * PRIME + ($actualCarrierFlight4 == null ? 43 : $actualCarrierFlight4.hashCode());
        final Object $departureTimeCpn4 = this.getDepartureTimeCpn4();
        result = result * PRIME + ($departureTimeCpn4 == null ? 43 : $departureTimeCpn4.hashCode());
        final Object $ticketSegment4 = this.getTicketSegment4();
        result = result * PRIME + ($ticketSegment4 == null ? 43 : $ticketSegment4.hashCode());
        final Object $actualCarrierSegment4 = this.getActualCarrierSegment4();
        result = result * PRIME + ($actualCarrierSegment4 == null ? 43 : $actualCarrierSegment4.hashCode());
        final Object $ticketTravelDate4 = this.getTicketTravelDate4();
        result = result * PRIME + ($ticketTravelDate4 == null ? 43 : $ticketTravelDate4.hashCode());
        final Object $transportProcessMonth4 = this.getTransportProcessMonth4();
        result = result * PRIME + ($transportProcessMonth4 == null ? 43 : $transportProcessMonth4.hashCode());
        final Object $actualCarrierDate4 = this.getActualCarrierDate4();
        result = result * PRIME + ($actualCarrierDate4 == null ? 43 : $actualCarrierDate4.hashCode());
        final Object $transportSegmentType4 = this.getTransportSegmentType4();
        result = result * PRIME + ($transportSegmentType4 == null ? 43 : $transportSegmentType4.hashCode());
        final Object $ticketCabin4 = this.getTicketCabin4();
        result = result * PRIME + ($ticketCabin4 == null ? 43 : $ticketCabin4.hashCode());
        final Object $actualCabin4 = this.getActualCabin4();
        result = result * PRIME + ($actualCabin4 == null ? 43 : $actualCabin4.hashCode());
        final Object $saleFba4 = this.getSaleFba4();
        result = result * PRIME + ($saleFba4 == null ? 43 : $saleFba4.hashCode());
        final Object $transportFba4 = this.getTransportFba4();
        result = result * PRIME + ($transportFba4 == null ? 43 : $transportFba4.hashCode());
        final Object $saleApportionment4 = this.getSaleApportionment4();
        result = result * PRIME + ($saleApportionment4 == null ? 43 : $saleApportionment4.hashCode());
        final Object $saleFuel4 = this.getSaleFuel4();
        result = result * PRIME + ($saleFuel4 == null ? 43 : $saleFuel4.hashCode());
        final Object $transportApportionment4 = this.getTransportApportionment4();
        result = result * PRIME + ($transportApportionment4 == null ? 43 : $transportApportionment4.hashCode());
        final Object $transportFuel4 = this.getTransportFuel4();
        result = result * PRIME + ($transportFuel4 == null ? 43 : $transportFuel4.hashCode());
        final Object $saleCommission4 = this.getSaleCommission4();
        result = result * PRIME + ($saleCommission4 == null ? 43 : $saleCommission4.hashCode());
        final Object $etlCreateTime = this.getEtlCreateTime();
        result = result * PRIME + ($etlCreateTime == null ? 43 : $etlCreateTime.hashCode());
        final Object $etlUpdateTime = this.getEtlUpdateTime();
        result = result * PRIME + ($etlUpdateTime == null ? 43 : $etlUpdateTime.hashCode());
        final Object $etlDate = this.getEtlDate();
        result = result * PRIME + ($etlDate == null ? 43 : $etlDate.hashCode());
        return result;
    }

    public String toString() {
        return "TOdsYxwSaleReport(id=" + this.getId() + ", sdasrc=" + this.getSdasrc() + ", sdasta=" + this.getSdasta() + ", electronicTicketType=" + this.getElectronicTicketType() + ", agentCode=" + this.getAgentCode() + ", agentName=" + this.getAgentName() + ", saleType=" + this.getSaleType() + ", originalTicketNumber=" + this.getOriginalTicketNumber() + ", saleProcessMonth=" + this.getSaleProcessMonth() + ", saleDate=" + this.getSaleDate() + ", ticketNumber=" + this.getTicketNumber() + ", ticketRoute=" + this.getTicketRoute() + ", routeType=" + this.getRouteType() + ", tourCode=" + this.getTourCode() + ", foreignCarrierFlag=" + this.getForeignCarrierFlag() + ", groupIndividualFlag=" + this.getGroupIndividualFlag() + ", teamName=" + this.getTeamName() + ", passengerType=" + this.getPassengerType() + ", linkedTicketFlag=" + this.getLinkedTicketFlag() + ", frequentFlyerNumber=" + this.getFrequentFlyerNumber() + ", ticketGds=" + this.getTicketGds() + ", endorsement=" + this.getEndorsement() + ", grossAmount=" + this.getGrossAmount() + ", netAmount=" + this.getNetAmount() + ", standardAgentRate=" + this.getStandardAgentRate() + ", standardAgentFee=" + this.getStandardAgentFee() + ", extraAgentRate=" + this.getExtraAgentRate() + ", extraAgentFee=" + this.getExtraAgentFee() + ", segmentNo1=" + this.getSegmentNo1() + ", stopover1=" + this.getStopover1() + ", mcCarrier1=" + this.getMcCarrier1() + ", mcFlight1=" + this.getMcFlight1() + ", ocCarrier1=" + this.getOcCarrier1() + ", ocFlight1=" + this.getOcFlight1() + ", saleSegmentType1=" + this.getSaleSegmentType1() + ", domesticForeignCarrierFlag1=" + this.getDomesticForeignCarrierFlag1() + ", actualCarrier1=" + this.getActualCarrier1() + ", actualCarrierFlight1=" + this.getActualCarrierFlight1() + ", departureTime1=" + this.getDepartureTime1() + ", ticketSegment1=" + this.getTicketSegment1() + ", actualCarrierSegment1=" + this.getActualCarrierSegment1() + ", ticketTravelDate1=" + this.getTicketTravelDate1() + ", transportProcessMonth1=" + this.getTransportProcessMonth1() + ", actualCarrierDate1=" + this.getActualCarrierDate1() + ", transportSegmentType1=" + this.getTransportSegmentType1() + ", ticketCabin1=" + this.getTicketCabin1() + ", actualCabin1=" + this.getActualCabin1() + ", saleFba1=" + this.getSaleFba1() + ", transportFba1=" + this.getTransportFba1() + ", saleApportionment1=" + this.getSaleApportionment1() + ", saleFuel1=" + this.getSaleFuel1() + ", transportApportionment1=" + this.getTransportApportionment1() + ", transportFuel1=" + this.getTransportFuel1() + ", saleCommission1=" + this.getSaleCommission1() + ", segmentNo2=" + this.getSegmentNo2() + ", stopover2=" + this.getStopover2() + ", mcCarrier2=" + this.getMcCarrier2() + ", mcFlight2=" + this.getMcFlight2() + ", ocCarrier2=" + this.getOcCarrier2() + ", ocFlight2=" + this.getOcFlight2() + ", saleSegmentType2=" + this.getSaleSegmentType2() + ", domesticForeignCarrierFlag2=" + this.getDomesticForeignCarrierFlag2() + ", actualCarrier2=" + this.getActualCarrier2() + ", actualCarrierFlight2=" + this.getActualCarrierFlight2() + ", departureTime2=" + this.getDepartureTime2() + ", ticketSegment2=" + this.getTicketSegment2() + ", actualCarrierSegmentCpn2=" + this.getActualCarrierSegmentCpn2() + ", ticketTravelDate2=" + this.getTicketTravelDate2() + ", transportProcessMonth2=" + this.getTransportProcessMonth2() + ", actualCarrierDateCpn2=" + this.getActualCarrierDateCpn2() + ", transportSegmentTypeCpn2=" + this.getTransportSegmentTypeCpn2() + ", ticketCabin2=" + this.getTicketCabin2() + ", actualCabinCpn2=" + this.getActualCabinCpn2() + ", saleFba2=" + this.getSaleFba2() + ", transportFba2=" + this.getTransportFba2() + ", saleApportionment2=" + this.getSaleApportionment2() + ", saleFuel2=" + this.getSaleFuel2() + ", transportApportionment2=" + this.getTransportApportionment2() + ", transportFuel2=" + this.getTransportFuel2() + ", saleCommission2=" + this.getSaleCommission2() + ", segmentNo3=" + this.getSegmentNo3() + ", stopover3=" + this.getStopover3() + ", mcCarrier3=" + this.getMcCarrier3() + ", mcFlight3=" + this.getMcFlight3() + ", ocCarrier3=" + this.getOcCarrier3() + ", ocFlight3=" + this.getOcFlight3() + ", saleSegmentType3=" + this.getSaleSegmentType3() + ", domesticForeignCarrierFlag3=" + this.getDomesticForeignCarrierFlag3() + ", actualCarrier3=" + this.getActualCarrier3() + ", actualCarrierFlight3=" + this.getActualCarrierFlight3() + ", departureTimeCpn3=" + this.getDepartureTimeCpn3() + ", ticketSegmentCpn3=" + this.getTicketSegmentCpn3() + ", actualCarrierSegmentCpn3=" + this.getActualCarrierSegmentCpn3() + ", ticketTravelDate3=" + this.getTicketTravelDate3() + ", transportProcessMonth3=" + this.getTransportProcessMonth3() + ", actualCarrierDate3=" + this.getActualCarrierDate3() + ", transportSegmentType3=" + this.getTransportSegmentType3() + ", ticketCabin3=" + this.getTicketCabin3() + ", actualCabin3=" + this.getActualCabin3() + ", saleFba3=" + this.getSaleFba3() + ", transportFba3=" + this.getTransportFba3() + ", saleApportionment3=" + this.getSaleApportionment3() + ", saleFuel3=" + this.getSaleFuel3() + ", transportApportionment3=" + this.getTransportApportionment3() + ", transportFuel3=" + this.getTransportFuel3() + ", saleCommission3=" + this.getSaleCommission3() + ", segmentNo4=" + this.getSegmentNo4() + ", stopover4=" + this.getStopover4() + ", mcCarrier4=" + this.getMcCarrier4() + ", mcFlight4=" + this.getMcFlight4() + ", ocCarrier4=" + this.getOcCarrier4() + ", ocFlight4=" + this.getOcFlight4() + ", saleSegmentType4=" + this.getSaleSegmentType4() + ", domesticForeignCarrierFlag4=" + this.getDomesticForeignCarrierFlag4() + ", actualCarrier4=" + this.getActualCarrier4() + ", actualCarrierFlight4=" + this.getActualCarrierFlight4() + ", departureTimeCpn4=" + this.getDepartureTimeCpn4() + ", ticketSegment4=" + this.getTicketSegment4() + ", actualCarrierSegment4=" + this.getActualCarrierSegment4() + ", ticketTravelDate4=" + this.getTicketTravelDate4() + ", transportProcessMonth4=" + this.getTransportProcessMonth4() + ", actualCarrierDate4=" + this.getActualCarrierDate4() + ", transportSegmentType4=" + this.getTransportSegmentType4() + ", ticketCabin4=" + this.getTicketCabin4() + ", actualCabin4=" + this.getActualCabin4() + ", saleFba4=" + this.getSaleFba4() + ", transportFba4=" + this.getTransportFba4() + ", saleApportionment4=" + this.getSaleApportionment4() + ", saleFuel4=" + this.getSaleFuel4() + ", transportApportionment4=" + this.getTransportApportionment4() + ", transportFuel4=" + this.getTransportFuel4() + ", saleCommission4=" + this.getSaleCommission4() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }
}
