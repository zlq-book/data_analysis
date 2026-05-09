package com.travelsky.dataplatform.constans.trp.model;

public class MonitorTarget {
    private String alertTargetType;
    private ApplicationAlertTarget applicationAlertTarget = new ApplicationAlertTarget();
    private BusinessAlertTarget businessAlertTarget;

    /**
     * Gets the value of alertTargetType.
     *
     * @return the value of alertTargetType
     * @description: 获取 alertTargetType
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getAlertTargetType() {
        return alertTargetType;
    }

    /**
     * Sets the alertTargetType.
     *
     * <p>You can use getAlertTargetType() to get the value of alertTargetType</p>
     *
     * @param alertTargetType alertTargetType
     * @return the value of alertTargetType
     * @description: 获取 alertTargetType
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setAlertTargetType(String alertTargetType) {
        this.alertTargetType = alertTargetType;
    }

    /**
     * Gets the value of applicationAlertTarget.
     *
     * @return the value of applicationAlertTarget
     * @description: 获取 applicationAlertTarget
     * @author: Wanghy
     * @version: TRP-19694
     */
    public ApplicationAlertTarget getApplicationAlertTarget() {
        return applicationAlertTarget;
    }

    /**
     * Sets the applicationAlertTarget.
     *
     * <p>You can use getApplicationAlertTarget() to get the value of applicationAlertTarget</p>
     *
     * @param applicationAlertTarget applicationAlertTarget
     * @return the value of applicationAlertTarget
     * @description: 获取 applicationAlertTarget
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setApplicationAlertTarget(ApplicationAlertTarget applicationAlertTarget) {
        this.applicationAlertTarget = applicationAlertTarget;
    }

    /**
     * Gets the value of businessAlertTarget.
     *
     * @return the value of businessAlertTarget
     * @description: 获取 businessAlertTarget
     * @author: Wanghy
     * @version: TRP-19694
     */
    public BusinessAlertTarget getBusinessAlertTarget() {
        return businessAlertTarget;
    }

    /**
     * Sets the businessAlertTarget.
     *
     * <p>You can use getBusinessAlertTarget() to get the value of businessAlertTarget</p>
     *
     * @param businessAlertTarget businessAlertTarget
     * @return the value of businessAlertTarget
     * @description: 获取 businessAlertTarget
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setBusinessAlertTarget(BusinessAlertTarget businessAlertTarget) {
        this.businessAlertTarget = businessAlertTarget;
    }

    @Override
    public String toString() {
        return "MonitorTarget{" +
                "alertTargetType='" + alertTargetType + '\'' +
                ", applicationAlertTarget=" + applicationAlertTarget +
                ", businessAlertTarget=" + businessAlertTarget +
                '}';
    }
}
