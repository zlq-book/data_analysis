package com.travelsky.dataplatform.constans.trp.model;

public class PostEvent {
    private String id;
    private String source;
    private long time;
    private Data data = new Data();

    /**
     * Gets the value of id.
     *
     * @return the value of id
     * @description: 获取 id
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the id.
     *
     * <p>You can use getId() to get the value of id</p>
     *
     * @param id id
     * @return the value of id
     * @description: 获取 id
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the value of source.
     *
     * @return the value of source
     * @description: 获取 source
     * @author: Wanghy
     * @version: TRP-19694
     */
    public String getSource() {
        return source;
    }

    /**
     * Sets the source.
     *
     * <p>You can use getSource() to get the value of source</p>
     *
     * @param source source
     * @return the value of source
     * @description: 获取 source
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setSource(String source) {
        this.source = source;
    }

    /**
     * Gets the value of time.
     *
     * @return the value of time
     * @description: 获取 time
     * @author: Wanghy
     * @version: TRP-19694
     */
    public long getTime() {
        return time;
    }

    /**
     * Sets the time.
     *
     * <p>You can use getTime() to get the value of time</p>
     *
     * @param time time
     * @return the value of time
     * @description: 获取 time
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setTime(long time) {
        this.time = time;
    }

    /**
     * Gets the value of data.
     *
     * @return the value of data
     * @description: 获取 data
     * @author: Wanghy
     * @version: TRP-19694
     */
    public Data getData() {
        return data;
    }

    /**
     * Sets the data.
     *
     * <p>You can use getData() to get the value of data</p>
     *
     * @param data data
     * @return the value of data
     * @description: 获取 data
     * @author: Wanghy
     * @version: TRP-19694
     */
    public void setData(Data data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "PostEvent{" +
                "id='" + id + '\'' +
                ", source='" + source + '\'' +
                ", time=" + time +
                ", data=" + data +
                '}';
    }
}
