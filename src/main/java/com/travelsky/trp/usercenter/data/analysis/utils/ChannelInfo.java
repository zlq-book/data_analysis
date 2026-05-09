package com.travelsky.trp.usercenter.data.analysis.utils;

/**
 * 渠道信息模型
 * 包含1-4级渠道的code和name
 */
public class ChannelInfo {
    
    private String level1Code;
    private String level1Name;
    private String level2Code;
    private String level2Name;
    private String level3Code;
    private String level3Name;
    private String level4Code;
    private String level4Name;
    private String ffpDeptFlag;

    public ChannelInfo() {
    }

    public ChannelInfo(String level1Code, String level1Name, String level2Code, String level2Name,
                       String level3Code, String level3Name, String level4Code, String level4Name) {
        this.level1Code = level1Code;
        this.level1Name = level1Name;
        this.level2Code = level2Code;
        this.level2Name = level2Name;
        this.level3Code = level3Code;
        this.level3Name = level3Name;
        this.level4Code = level4Code;
        this.level4Name = level4Name;
    }

    public ChannelInfo(String level1Code, String level1Name, String level2Code, String level2Name,
                       String level3Code, String level3Name, String level4Code, String level4Name, String ffpDeptFlag) {
        this.level1Code = level1Code;
        this.level1Name = level1Name;
        this.level2Code = level2Code;
        this.level2Name = level2Name;
        this.level3Code = level3Code;
        this.level3Name = level3Name;
        this.level4Code = level4Code;
        this.level4Name = level4Name;
        this.ffpDeptFlag = ffpDeptFlag;
    }

    public String getLevel1Code() {
        return level1Code;
    }

    public void setLevel1Code(String level1Code) {
        this.level1Code = level1Code;
    }

    public String getLevel1Name() {
        return level1Name;
    }

    public void setLevel1Name(String level1Name) {
        this.level1Name = level1Name;
    }

    public String getLevel2Code() {
        return level2Code;
    }

    public void setLevel2Code(String level2Code) {
        this.level2Code = level2Code;
    }

    public String getLevel2Name() {
        return level2Name;
    }

    public void setLevel2Name(String level2Name) {
        this.level2Name = level2Name;
    }

    public String getLevel3Code() {
        return level3Code;
    }

    public void setLevel3Code(String level3Code) {
        this.level3Code = level3Code;
    }

    public String getLevel3Name() {
        return level3Name;
    }

    public void setLevel3Name(String level3Name) {
        this.level3Name = level3Name;
    }

    public String getLevel4Code() {
        return level4Code;
    }

    public void setLevel4Code(String level4Code) {
        this.level4Code = level4Code;
    }

    public String getLevel4Name() {
        return level4Name;
    }

    public void setLevel4Name(String level4Name) {
        this.level4Name = level4Name;
    }

    public String getFfpDeptFlag() {
        return ffpDeptFlag;
    }

    public void setFfpDeptFlag(String ffpDeptFlag) {
        this.ffpDeptFlag = ffpDeptFlag;
    }

    @Override
    public String toString() {
        return "ChannelInfo{" +
                "level1Code='" + level1Code + '\'' +
                ", level1Name='" + level1Name + '\'' +
                ", level2Code='" + level2Code + '\'' +
                ", level2Name='" + level2Name + '\'' +
                ", level3Code='" + level3Code + '\'' +
                ", level3Name='" + level3Name + '\'' +
                ", level4Code='" + level4Code + '\'' +
                ", level4Name='" + level4Name + '\'' +
                ", ffpDeptFlag='" + ffpDeptFlag + '\'' +
                '}';
    }
}

