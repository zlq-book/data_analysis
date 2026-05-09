package com.travelsky.dataplatform.constans.trp.model;

public class BusinessAlertTarget {
    private String businessTargetCode;

    /**
     * Gets the value of businessTargetCode.
     *
     * @return the value of businessTargetCode
     * @description: 获取 businessTargetCode
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getBusinessTargetCode() {
        return businessTargetCode;
    }

    /**
     * Sets the businessTargetCode.
     *
     * <p>You can use getBusinessTargetCode() to get the value of businessTargetCode</p>
     *
     * @param businessTargetCode businessTargetCode
     * @return the value of businessTargetCode
     * @description: 获取 businessTargetCode
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setBusinessTargetCode(String businessTargetCode) {
        this.businessTargetCode = businessTargetCode;
    }

    @Override
    public String toString() {
        return "BusinessTargetCode{" +
                "businessTargetCode='" + businessTargetCode + '\'' +
                '}';
    }
}
