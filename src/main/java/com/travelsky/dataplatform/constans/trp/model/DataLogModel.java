package com.travelsky.dataplatform.constans.trp.model;


import com.travelsky.dataplatform.constans.trp.annotation.Column;
import com.travelsky.dataplatform.constans.trp.annotation.RowKey;

/**
 * data_log表实例
 */
public class DataLogModel {

    @RowKey
    private String dataDate;
    @Column
    private String itinearyExtract;
    @Column
    private String searchExtract;
    @Column
    private String orderSsearchLoad;
    @Column
    private String orderFetchLoad;
    @Column
    private String summaryFetchSuccess;
    @Column
    private String summaryFetchFail;
    @Column
    private String spnrFetchSuccess;
    @Column
    private String spnrFetchFail;
    @Column
    private String deletCount;

    public DataLogModel() {
    }

    /**
     * Gets the value of dataDate.
     *
     * @return the value of dataDate
     * @description: 获取 dataDate
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getDataDate() {
        return dataDate;
    }

    /**
     * Sets the dataDate.
     *
     * <p>You can use getDataDate() to get the value of dataDate</p>
     *
     * @param dataDate dataDate
     * @return the value of dataDate
     * @description: 获取 dataDate
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setDataDate(String dataDate) {
        this.dataDate = dataDate;
    }

    /**
     * Gets the value of itinearyExtract.
     *
     * @return the value of itinearyExtract
     * @description: 获取 itinearyExtract
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getItinearyExtract() {
        return itinearyExtract;
    }

    /**
     * Sets the itinearyExtract.
     *
     * <p>You can use getItinearyExtract() to get the value of itinearyExtract</p>
     *
     * @param itinearyExtract itinearyExtract
     * @return the value of itinearyExtract
     * @description: 获取 itinearyExtract
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setItinearyExtract(String itinearyExtract) {
        this.itinearyExtract = itinearyExtract;
    }

    /**
     * Gets the value of searchExtract.
     *
     * @return the value of searchExtract
     * @description: 获取 searchExtract
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getSearchExtract() {
        return searchExtract;
    }

    /**
     * Sets the searchExtract.
     *
     * <p>You can use getSearchExtract() to get the value of searchExtract</p>
     *
     * @param searchExtract searchExtract
     * @return the value of searchExtract
     * @description: 获取 searchExtract
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setSearchExtract(String searchExtract) {
        this.searchExtract = searchExtract;
    }

    /**
     * Gets the value of orderSsearchLoad.
     *
     * @return the value of orderSsearchLoad
     * @description: 获取 orderSsearchLoad
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getOrderSsearchLoad() {
        return orderSsearchLoad;
    }

    /**
     * Sets the orderSsearchLoad.
     *
     * <p>You can use getOrderSsearchLoad() to get the value of orderSsearchLoad</p>
     *
     * @param orderSsearchLoad orderSsearchLoad
     * @return the value of orderSsearchLoad
     * @description: 获取 orderSsearchLoad
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setOrderSsearchLoad(String orderSsearchLoad) {
        this.orderSsearchLoad = orderSsearchLoad;
    }

    /**
     * Gets the value of orderFetchLoad.
     *
     * @return the value of orderFetchLoad
     * @description: 获取 orderFetchLoad
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getOrderFetchLoad() {
        return orderFetchLoad;
    }

    /**
     * Sets the orderFetchLoad.
     *
     * <p>You can use getOrderFetchLoad() to get the value of orderFetchLoad</p>
     *
     * @param orderFetchLoad orderFetchLoad
     * @return the value of orderFetchLoad
     * @description: 获取 orderFetchLoad
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setOrderFetchLoad(String orderFetchLoad) {
        this.orderFetchLoad = orderFetchLoad;
    }

    /**
     * Gets the value of summaryFetchSuccess.
     *
     * @return the value of summaryFetchSuccess
     * @description: 获取 summaryFetchSuccess
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getSummaryFetchSuccess() {
        return summaryFetchSuccess;
    }

    /**
     * Sets the summaryFetchSuccess.
     *
     * <p>You can use getSummaryFetchSuccess() to get the value of summaryFetchSuccess</p>
     *
     * @param summaryFetchSuccess summaryFetchSuccess
     * @return the value of summaryFetchSuccess
     * @description: 获取 summaryFetchSuccess
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setSummaryFetchSuccess(String summaryFetchSuccess) {
        this.summaryFetchSuccess = summaryFetchSuccess;
    }

    /**
     * Gets the value of summaryFetchFail.
     *
     * @return the value of summaryFetchFail
     * @description: 获取 summaryFetchFail
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getSummaryFetchFail() {
        return summaryFetchFail;
    }

    /**
     * Sets the summaryFetchFail.
     *
     * <p>You can use getSummaryFetchFail() to get the value of summaryFetchFail</p>
     *
     * @param summaryFetchFail summaryFetchFail
     * @return the value of summaryFetchFail
     * @description: 获取 summaryFetchFail
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setSummaryFetchFail(String summaryFetchFail) {
        this.summaryFetchFail = summaryFetchFail;
    }

    /**
     * Gets the value of spnrFetchSuccess.
     *
     * @return the value of spnrFetchSuccess
     * @description: 获取 spnrFetchSuccess
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getSpnrFetchSuccess() {
        return spnrFetchSuccess;
    }

    /**
     * Sets the spnrFetchSuccess.
     *
     * <p>You can use getSpnrFetchSuccess() to get the value of spnrFetchSuccess</p>
     *
     * @param spnrFetchSuccess spnrFetchSuccess
     * @return the value of spnrFetchSuccess
     * @description: 获取 spnrFetchSuccess
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setSpnrFetchSuccess(String spnrFetchSuccess) {
        this.spnrFetchSuccess = spnrFetchSuccess;
    }

    /**
     * Gets the value of spnrFetchFail.
     *
     * @return the value of spnrFetchFail
     * @description: 获取 spnrFetchFail
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getSpnrFetchFail() {
        return spnrFetchFail;
    }

    /**
     * Sets the spnrFetchFail.
     *
     * <p>You can use getSpnrFetchFail() to get the value of spnrFetchFail</p>
     *
     * @param spnrFetchFail spnrFetchFail
     * @return the value of spnrFetchFail
     * @description: 获取 spnrFetchFail
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setSpnrFetchFail(String spnrFetchFail) {
        this.spnrFetchFail = spnrFetchFail;
    }

    /**
     * Gets the value of deletCount.
     *
     * @return the value of deletCount
     * @description: 获取 deletCount
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getDeletCount() {
        return deletCount;
    }

    /**
     * Sets the deletCount.
     *
     * <p>You can use getDeletCount() to get the value of deletCount</p>
     *
     * @param deletCount deletCount
     * @return the value of deletCount
     * @description: 获取 deletCount
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setDeletCount(String deletCount) {
        this.deletCount = deletCount;
    }

    @Override
    public String toString() {
        return "DataLogModel{" +
                "dataDate='" + dataDate + '\'' +
                ", itinearyExtract='" + itinearyExtract + '\'' +
                ", searchExtract='" + searchExtract + '\'' +
                ", orderSsearchLoad='" + orderSsearchLoad + '\'' +
                ", orderFetchLoad='" + orderFetchLoad + '\'' +
                ", summaryFetchSuccess='" + summaryFetchSuccess + '\'' +
                ", summaryFetchFail='" + summaryFetchFail + '\'' +
                ", spnrFetchSuccess='" + spnrFetchSuccess + '\'' +
                ", spnrFetchFail='" + spnrFetchFail + '\'' +
                ", deletCount='" + deletCount + '\'' +
                '}';
    }
}
