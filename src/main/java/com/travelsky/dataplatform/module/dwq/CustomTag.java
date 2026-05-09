package com.travelsky.dataplatform.module.dwq;

public class CustomTag {
    private String tagName;
    private String tagShowName;

    public CustomTag() {
    }
    public CustomTag(String tagName, String tagShowName) {
        this.tagName = tagName;
        this.tagShowName = tagShowName;
    }
    public String getTagName() {
        return tagName;
    }

    public void setTagName(String tagName) {
        this.tagName = tagName;
    }

    public String getTagShowName() {
        return tagShowName;
    }

    public void setTagShowName(String tagShowName) {
        this.tagShowName = tagShowName;
    }

    @Override
    public String toString() {
        return "CustomTag{" +
                "tagName='" + tagName + '\'' +
                ", tagShowName='" + tagShowName + '\'' +
                '}';
    }
}
