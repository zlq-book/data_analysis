package com.travelsky.dataplatform.module.ods.clk;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.math.BigDecimal;

/**
 * 国航系里程累积明细 实体类
 * 用于读取和写入国航系里程累积明细报表Excel文件
 */
public class TOdsClkMileageAccumulation implements java.io.Serializable {

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
     * 飞行
     */
    @ExcelProperty("飞行")
    private BigDecimal flight;

    /**
     * 非航
     */
    @ExcelProperty("非航")
    private BigDecimal nonFlight;

    /**
     * 促销
     */
    @ExcelProperty("促销")
    private BigDecimal promotion;

    /**
     * 额外
     */
    @ExcelProperty("额外")
    private BigDecimal extra;

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

    public TOdsClkMileageAccumulation() {
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

    public BigDecimal getFlight() {
        return this.flight;
    }

    public BigDecimal getNonFlight() {
        return this.nonFlight;
    }

    public BigDecimal getPromotion() {
        return this.promotion;
    }

    public BigDecimal getExtra() {
        return this.extra;
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

    public void setFlight(BigDecimal flight) {
        this.flight = flight;
    }

    public void setNonFlight(BigDecimal nonFlight) {
        this.nonFlight = nonFlight;
    }

    public void setPromotion(BigDecimal promotion) {
        this.promotion = promotion;
    }

    public void setExtra(BigDecimal extra) {
        this.extra = extra;
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
        if (!(o instanceof TOdsClkMileageAccumulation)) return false;
        final TOdsClkMileageAccumulation other = (TOdsClkMileageAccumulation) o;
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
        final Object this$flight = this.getFlight();
        final Object other$flight = other.getFlight();
        if (this$flight == null ? other$flight != null : !this$flight.equals(other$flight)) return false;
        final Object this$nonFlight = this.getNonFlight();
        final Object other$nonFlight = other.getNonFlight();
        if (this$nonFlight == null ? other$nonFlight != null : !this$nonFlight.equals(other$nonFlight)) return false;
        final Object this$promotion = this.getPromotion();
        final Object other$promotion = other.getPromotion();
        if (this$promotion == null ? other$promotion != null : !this$promotion.equals(other$promotion)) return false;
        final Object this$extra = this.getExtra();
        final Object other$extra = other.getExtra();
        if (this$extra == null ? other$extra != null : !this$extra.equals(other$extra)) return false;
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
        return other instanceof TOdsClkMileageAccumulation;
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
        final Object $flight = this.getFlight();
        result = result * PRIME + ($flight == null ? 43 : $flight.hashCode());
        final Object $nonFlight = this.getNonFlight();
        result = result * PRIME + ($nonFlight == null ? 43 : $nonFlight.hashCode());
        final Object $promotion = this.getPromotion();
        result = result * PRIME + ($promotion == null ? 43 : $promotion.hashCode());
        final Object $extra = this.getExtra();
        result = result * PRIME + ($extra == null ? 43 : $extra.hashCode());
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
        return "TOdsClkMileageAccumulation(id=" + this.getId() + ", date=" + this.getDate() + ", airline=" + this.getAirline() + ", flight=" + this.getFlight() + ", nonFlight=" + this.getNonFlight() + ", promotion=" + this.getPromotion() + ", extra=" + this.getExtra() + ", total=" + this.getTotal() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }
}