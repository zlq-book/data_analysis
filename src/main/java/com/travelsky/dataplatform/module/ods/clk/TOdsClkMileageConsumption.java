package com.travelsky.dataplatform.module.ods.clk;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.math.BigDecimal;

/**
 * 国航系里程消费明细 实体类
 * 用于读取和写入国航系里程消费明细报表Excel文件
 */
public class TOdsClkMileageConsumption implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 主键ID
     */
    @ExcelIgnore
    private Long id;


    /**
     * 日期
     */
    @ExcelProperty("日期")
    private String date;

    /**
     * 航司
     */
    @ExcelProperty("航司")
    private String airline;

    /**
     * 免票
     */
    @ExcelProperty("免票")
    private BigDecimal freeTicket;

    /**
     * 升舱
     */
    @ExcelProperty("升舱")
    private BigDecimal cabinUpgrade;

    /**
     * 非航
     */
    @ExcelProperty("非航")
    private BigDecimal nonFlight;

    /**
     * 总
     */
    @ExcelProperty("总")
    private BigDecimal total;


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

    public TOdsClkMileageConsumption() {
    }

    public Long getId() {
        return this.id;
    }

    public String getDate() {
        return this.date;
    }

    public String getAirline() {
        return this.airline;
    }

    public BigDecimal getFreeTicket() {
        return this.freeTicket;
    }

    public BigDecimal getCabinUpgrade() {
        return this.cabinUpgrade;
    }

    public BigDecimal getNonFlight() {
        return this.nonFlight;
    }

    public BigDecimal getTotal() {
        return this.total;
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

    public void setDate(String date) {
        this.date = date;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public void setFreeTicket(BigDecimal freeTicket) {
        this.freeTicket = freeTicket;
    }

    public void setCabinUpgrade(BigDecimal cabinUpgrade) {
        this.cabinUpgrade = cabinUpgrade;
    }

    public void setNonFlight(BigDecimal nonFlight) {
        this.nonFlight = nonFlight;
    }

    public void setTotal(BigDecimal total) {
        this.total = total;
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
        if (!(o instanceof TOdsClkMileageConsumption)) return false;
        final TOdsClkMileageConsumption other = (TOdsClkMileageConsumption) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$id = this.getId();
        final Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
        final Object this$date = this.getDate();
        final Object other$date = other.getDate();
        if (this$date == null ? other$date != null : !this$date.equals(other$date)) return false;
        final Object this$airline = this.getAirline();
        final Object other$airline = other.getAirline();
        if (this$airline == null ? other$airline != null : !this$airline.equals(other$airline)) return false;
        final Object this$freeTicket = this.getFreeTicket();
        final Object other$freeTicket = other.getFreeTicket();
        if (this$freeTicket == null ? other$freeTicket != null : !this$freeTicket.equals(other$freeTicket))
            return false;
        final Object this$cabinUpgrade = this.getCabinUpgrade();
        final Object other$cabinUpgrade = other.getCabinUpgrade();
        if (this$cabinUpgrade == null ? other$cabinUpgrade != null : !this$cabinUpgrade.equals(other$cabinUpgrade))
            return false;
        final Object this$nonFlight = this.getNonFlight();
        final Object other$nonFlight = other.getNonFlight();
        if (this$nonFlight == null ? other$nonFlight != null : !this$nonFlight.equals(other$nonFlight)) return false;
        final Object this$total = this.getTotal();
        final Object other$total = other.getTotal();
        if (this$total == null ? other$total != null : !this$total.equals(other$total)) return false;
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
        return other instanceof TOdsClkMileageConsumption;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final Object $date = this.getDate();
        result = result * PRIME + ($date == null ? 43 : $date.hashCode());
        final Object $airline = this.getAirline();
        result = result * PRIME + ($airline == null ? 43 : $airline.hashCode());
        final Object $freeTicket = this.getFreeTicket();
        result = result * PRIME + ($freeTicket == null ? 43 : $freeTicket.hashCode());
        final Object $cabinUpgrade = this.getCabinUpgrade();
        result = result * PRIME + ($cabinUpgrade == null ? 43 : $cabinUpgrade.hashCode());
        final Object $nonFlight = this.getNonFlight();
        result = result * PRIME + ($nonFlight == null ? 43 : $nonFlight.hashCode());
        final Object $total = this.getTotal();
        result = result * PRIME + ($total == null ? 43 : $total.hashCode());
        final Object $etlCreateTime = this.getEtlCreateTime();
        result = result * PRIME + ($etlCreateTime == null ? 43 : $etlCreateTime.hashCode());
        final Object $etlUpdateTime = this.getEtlUpdateTime();
        result = result * PRIME + ($etlUpdateTime == null ? 43 : $etlUpdateTime.hashCode());
        final Object $etlDate = this.getEtlDate();
        result = result * PRIME + ($etlDate == null ? 43 : $etlDate.hashCode());
        return result;
    }

    public String toString() {
        return "TOdsClkMileageConsumption(id=" + this.getId() + ", date=" + this.getDate() + ", airline=" + this.getAirline() + ", freeTicket=" + this.getFreeTicket() + ", cabinUpgrade=" + this.getCabinUpgrade() + ", nonFlight=" + this.getNonFlight() + ", total=" + this.getTotal() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }
}