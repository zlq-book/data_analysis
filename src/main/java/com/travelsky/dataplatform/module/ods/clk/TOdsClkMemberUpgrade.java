package com.travelsky.dataplatform.module.ods.clk;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.math.BigDecimal;

/**
 * 国航系会员升级量 实体类
 * 用于读取和写入国航系会员升级量报表Excel文件
 */
public class TOdsClkMemberUpgrade implements java.io.Serializable {

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
     * 名称
     */
    @ExcelProperty("名称")
    private String name;

    /**
     * 终白
     */
    @ExcelProperty("终白")
    private BigDecimal finalWhite;

    /**
     * 白金
     */
    @ExcelProperty("白金")
    private BigDecimal platinum;

    /**
     * 金
     */
    @ExcelProperty("金")
    private BigDecimal gold;

    /**
     * 银
     */
    @ExcelProperty("银")
    private BigDecimal silver;

    /**
     * 小计
     */
    @ExcelProperty("小计")
    private BigDecimal subtotal;

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

    public TOdsClkMemberUpgrade() {
    }

    public Long getId() {
        return this.id;
    }

    public String getDate() {
        return this.date;
    }

    public String getName() {
        return this.name;
    }

    public BigDecimal getFinalWhite() {
        return this.finalWhite;
    }

    public BigDecimal getPlatinum() {
        return this.platinum;
    }

    public BigDecimal getGold() {
        return this.gold;
    }

    public BigDecimal getSilver() {
        return this.silver;
    }

    public BigDecimal getSubtotal() {
        return this.subtotal;
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

    public void setName(String name) {
        this.name = name;
    }

    public void setFinalWhite(BigDecimal finalWhite) {
        this.finalWhite = finalWhite;
    }

    public void setPlatinum(BigDecimal platinum) {
        this.platinum = platinum;
    }

    public void setGold(BigDecimal gold) {
        this.gold = gold;
    }

    public void setSilver(BigDecimal silver) {
        this.silver = silver;
    }

    public void setSubtotal(BigDecimal subtotal) {
        this.subtotal = subtotal;
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
        if (!(o instanceof TOdsClkMemberUpgrade)) return false;
        final TOdsClkMemberUpgrade other = (TOdsClkMemberUpgrade) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$id = this.getId();
        final Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
        final Object this$date = this.getDate();
        final Object other$date = other.getDate();
        if (this$date == null ? other$date != null : !this$date.equals(other$date)) return false;
        final Object this$name = this.getName();
        final Object other$name = other.getName();
        if (this$name == null ? other$name != null : !this$name.equals(other$name)) return false;
        final Object this$finalWhite = this.getFinalWhite();
        final Object other$finalWhite = other.getFinalWhite();
        if (this$finalWhite == null ? other$finalWhite != null : !this$finalWhite.equals(other$finalWhite))
            return false;
        final Object this$platinum = this.getPlatinum();
        final Object other$platinum = other.getPlatinum();
        if (this$platinum == null ? other$platinum != null : !this$platinum.equals(other$platinum)) return false;
        final Object this$gold = this.getGold();
        final Object other$gold = other.getGold();
        if (this$gold == null ? other$gold != null : !this$gold.equals(other$gold)) return false;
        final Object this$silver = this.getSilver();
        final Object other$silver = other.getSilver();
        if (this$silver == null ? other$silver != null : !this$silver.equals(other$silver)) return false;
        final Object this$subtotal = this.getSubtotal();
        final Object other$subtotal = other.getSubtotal();
        if (this$subtotal == null ? other$subtotal != null : !this$subtotal.equals(other$subtotal)) return false;
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
        return other instanceof TOdsClkMemberUpgrade;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final Object $date = this.getDate();
        result = result * PRIME + ($date == null ? 43 : $date.hashCode());
        final Object $name = this.getName();
        result = result * PRIME + ($name == null ? 43 : $name.hashCode());
        final Object $finalWhite = this.getFinalWhite();
        result = result * PRIME + ($finalWhite == null ? 43 : $finalWhite.hashCode());
        final Object $platinum = this.getPlatinum();
        result = result * PRIME + ($platinum == null ? 43 : $platinum.hashCode());
        final Object $gold = this.getGold();
        result = result * PRIME + ($gold == null ? 43 : $gold.hashCode());
        final Object $silver = this.getSilver();
        result = result * PRIME + ($silver == null ? 43 : $silver.hashCode());
        final Object $subtotal = this.getSubtotal();
        result = result * PRIME + ($subtotal == null ? 43 : $subtotal.hashCode());
        final Object $etlCreateTime = this.getEtlCreateTime();
        result = result * PRIME + ($etlCreateTime == null ? 43 : $etlCreateTime.hashCode());
        final Object $etlUpdateTime = this.getEtlUpdateTime();
        result = result * PRIME + ($etlUpdateTime == null ? 43 : $etlUpdateTime.hashCode());
        final Object $etlDate = this.getEtlDate();
        result = result * PRIME + ($etlDate == null ? 43 : $etlDate.hashCode());
        return result;
    }

    public String toString() {
        return "TOdsClkMemberUpgrade(id=" + this.getId() + ", date=" + this.getDate() + ", name=" + this.getName() + ", finalWhite=" + this.getFinalWhite() + ", platinum=" + this.getPlatinum() + ", gold=" + this.getGold() + ", silver=" + this.getSilver() + ", subtotal=" + this.getSubtotal() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }
}