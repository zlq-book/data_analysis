
package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * 高频实时处理数据中间表实体类-T_DWD_HSD_PROCESS_DATA
 * @author
 * @date
 */
public class HsdProcessDataModel {
    /**
     * 事件自增ID
     */
    @JsonProperty("EVENT_ID")
    private Long eventId;

    /**
     * 事件类型
     */
    @JsonProperty("EVENT")
    private String event;

    /**
     * 子事件类型
     */
    @JsonProperty("SUB_EVENT")
    private String subEvent;

    /**
     * 事实表名
     */
    @JsonProperty("TABLE_NAME")
    private String tableName;

    /**
     * 报文处理的时间
     */
    @JsonProperty("UPTM")
    private String uptm;

    /**
     * 报文处理串
     */
    @JsonProperty("STAMP")
    private String stamp;

    /**
     * 内容详情json
     */
    @JsonProperty("CONTENT")
    private String content;

    /**
     * 是否处理完成
     */
    @JsonProperty("IS_PROCESSED")
    private Boolean isProcessed;

    /**
     * 创建时间
     */
    @JsonProperty("CREATE_TIME")
    private String createTime = LocalDateTime.now().toString();

    /**
     * 更新时间
     */
    @JsonProperty("UPDATE_TIME")
    private String updateTime;

    public Long getEventId() {
        return eventId;
    }

    public void setEventId(Long eventId) {
        this.eventId = eventId;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getSubEvent() {
        return subEvent;
    }

    public void setSubEvent(String subEvent) {
        this.subEvent = subEvent;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUptm() {
        return uptm;
    }

    public void setUptm(String uptm) {
        this.uptm = uptm;
    }

    public String getStamp() {
        return stamp;
    }

    public void setStamp(String stamp) {
        this.stamp = stamp;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Boolean getProcessed() {
        return isProcessed;
    }

    public void setProcessed(Boolean processed) {
        isProcessed = processed;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "HsdProcessDataModel{" +
                "eventId=" + eventId +
                ", event='" + event + '\'' +
                ", subEvent='" + subEvent + '\'' +
                ", tableName='" + tableName + '\'' +
                ", uptms='" + uptm + '\'' +
                ", stamp='" + stamp + '\'' +
                ", content='" + content + '\'' +
                ", isProcessed=" + isProcessed +
                ", createTime='" + createTime + '\'' +
                ", updateTime='" + updateTime + '\'' +
                '}';
    }
}