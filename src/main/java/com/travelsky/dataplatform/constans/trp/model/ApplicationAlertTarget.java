package com.travelsky.dataplatform.constans.trp.model;

public class ApplicationAlertTarget {
    private String instanceCode;
    private String instanceName;
    private String ip;

    /**
     * Gets the value of instanceCode.
     *
     * @return the value of instanceCode
     * @description: 获取 instanceCode
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getInstanceCode() {
        return instanceCode;
    }

    /**
     * Sets the instanceCode.
     *
     * <p>You can use getInstanceCode() to get the value of instanceCode</p>
     *
     * @param instanceCode instanceCode
     * @return the value of instanceCode
     * @description: 获取 instanceCode
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setInstanceCode(String instanceCode) {
        this.instanceCode = instanceCode;
    }

    /**
     * Gets the value of instanceName.
     *
     * @return the value of instanceName
     * @description: 获取 instanceName
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getInstanceName() {
        return instanceName;
    }

    /**
     * Sets the instanceName.
     *
     * <p>You can use getInstanceName() to get the value of instanceName</p>
     *
     * @param instanceName instanceName
     * @return the value of instanceName
     * @description: 获取 instanceName
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    /**
     * Gets the value of ip.
     *
     * @return the value of ip
     * @description: 获取 ip
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getIp() {
        return ip;
    }

    /**
     * Sets the ip.
     *
     * <p>You can use getIp() to get the value of ip</p>
     *
     * @param ip ip
     * @return the value of ip
     * @description: 获取 ip
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setIp(String ip) {
        this.ip = ip;
    }

    @Override
    public String toString() {
        return "ApplicationAlertTarget{" +
                "instanceCode='" + instanceCode + '\'' +
                ", instanceName='" + instanceName + '\'' +
                ", ip='" + ip + '\'' +
                '}';
    }
}
