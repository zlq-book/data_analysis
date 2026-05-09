package com.travelsky.dataplatform.constans.trp.model;

public class Data {
    private String name;
    private String airlineCode;
    private String environment;
    private String productCode;
    private String applicationCode;
    private MonitorTarget monitorTarget = new MonitorTarget();
    private String eventStatus;
    private String content;

    /**
     * Gets the value of name.
     *
     * @return the value of name
     * @description: 获取 name
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name.
     *
     * <p>You can use getName() to get the value of name</p>
     *
     * @param name name
     * @return the value of name
     * @description: 获取 name
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the value of airlineCode.
     *
     * @return the value of airlineCode
     * @description: 获取 airlineCode
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getAirlineCode() {
        return airlineCode;
    }

    /**
     * Sets the airlineCode.
     *
     * <p>You can use getAirlineCode() to get the value of airlineCode</p>
     *
     * @param airlineCode airlineCode
     * @return the value of airlineCode
     * @description: 获取 airlineCode
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setAirlineCode(String airlineCode) {
        this.airlineCode = airlineCode;
    }

    /**
     * Gets the value of environment.
     *
     * @return the value of environment
     * @description: 获取 environment
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getEnvironment() {
        return environment;
    }

    /**
     * Sets the environment.
     *
     * <p>You can use getEnvironment() to get the value of environment</p>
     *
     * @param environment environment
     * @return the value of environment
     * @description: 获取 environment
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    /**
     * Gets the value of productCode.
     *
     * @return the value of productCode
     * @description: 获取 productCode
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getProductCode() {
        return productCode;
    }

    /**
     * Sets the productCode.
     *
     * <p>You can use getProductCode() to get the value of productCode</p>
     *
     * @param productCode productCode
     * @return the value of productCode
     * @description: 获取 productCode
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    /**
     * Gets the value of applicationCode.
     *
     * @return the value of applicationCode
     * @description: 获取 applicationCode
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getApplicationCode() {
        return applicationCode;
    }

    /**
     * Sets the applicationCode.
     *
     * <p>You can use getApplicationCode() to get the value of applicationCode</p>
     *
     * @param applicationCode applicationCode
     * @return the value of applicationCode
     * @description: 获取 applicationCode
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setApplicationCode(String applicationCode) {
        this.applicationCode = applicationCode;
    }

    /**
     * Gets the value of monitorTarget.
     *
     * @return the value of monitorTarget
     * @description: 获取 monitorTarget
     * @author: Wanghy
     * @version: TRP-19694
     */
    public MonitorTarget getMonitorTarget() {
        return monitorTarget;
    }

    /**
     * Sets the monitorTarget.
     *
     * <p>You can use getMonitorTarget() to get the value of monitorTarget</p>
     *
     * @param monitorTarget monitorTarget
     * @return the value of monitorTarget
     * @description: 获取 monitorTarget
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setMonitorTarget(MonitorTarget monitorTarget) {
        this.monitorTarget = monitorTarget;
    }

    /**
     * Gets the value of eventStatus.
     *
     * @return the value of eventStatus
     * @description: 获取 eventStatus
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getEventStatus() {
        return eventStatus;
    }

    /**
     * Sets the eventStatus.
     *
     * <p>You can use getEventStatus() to get the value of eventStatus</p>
     *
     * @param eventStatus eventStatus
     * @return the value of eventStatus
     * @description: 获取 eventStatus
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setEventStatus(String eventStatus) {
        this.eventStatus = eventStatus;
    }

    /**
     * Gets the value of content.
     *
     * @return the value of content
     * @description: 获取 content
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getContent() {
        return content;
    }

    /**
     * Sets the content.
     *
     * <p>You can use getContent() to get the value of content</p>
     *
     * @param content content
     * @return the value of content
     * @description: 获取 content
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "Data{" +
                "name='" + name + '\'' +
                ", airlineCode='" + airlineCode + '\'' +
                ", environment='" + environment + '\'' +
                ", productCode='" + productCode + '\'' +
                ", applicationCode='" + applicationCode + '\'' +
                ", monitorTarget=" + monitorTarget +
                ", eventStatus='" + eventStatus + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
