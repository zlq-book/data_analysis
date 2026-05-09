package com.travelsky.dataplatform.module.dim;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.doris.shaded.com.google.type.DateTime;

import java.sql.Date;


/**
 * @author kuangaihua
 * @date 2025/7/24 15:37
 */
public class TDimIdmappingError {
    @JsonProperty("ERROR_TID")
    private String ERROR_TID;

    @JsonProperty("SAME_STRONGID")
    private String SAME_STRONGID;

    @JsonProperty("DIFF_STRONGID")
    private String DIFF_STRONGID;

    @JsonProperty("ETL_DATE")
    private String ETL_DATE;

    @JsonProperty("CREATE_TIME")
    private String CREATE_TIME;

    @JsonProperty("UPDATE_TIME")
    private String UPDATE_TIME;

    public TDimIdmappingError() {
    }

    @Override
    public String toString() {
        return "TDimIdmappingError{" +
                "ERROR_TID='" + ERROR_TID + '\'' +
                ", SAME_STRONGID='" + SAME_STRONGID + '\'' +
                ", DIFF_STRONGID='" + DIFF_STRONGID + '\'' +
                ", ETL_DATE=" + ETL_DATE +
                ", CREATE_TIME='" + CREATE_TIME + '\'' +
                ", UPDATE_TIME='" + UPDATE_TIME + '\'' +
                '}';
    }

    public String getERROR_TID() {
        return ERROR_TID;
    }

    public void setERROR_TID(String ERROR_TID) {
        this.ERROR_TID = ERROR_TID;
    }

    public String getSAME_STRONGID() {
        return SAME_STRONGID;
    }

    public void setSAME_STRONGID(String SAME_STRONGID) {
        this.SAME_STRONGID = SAME_STRONGID;
    }

    public String getDIFF_STRONGID() {
        return DIFF_STRONGID;
    }

    public void setDIFF_STRONGID(String DIFF_STRONGID) {
        this.DIFF_STRONGID = DIFF_STRONGID;
    }

    public String getCREATE_TIME() {
        return CREATE_TIME;
    }

    public void setCREATE_TIME(String CREATE_TIME) {
        this.CREATE_TIME = CREATE_TIME;
    }

    public String getUPDATE_TIME() {
        return UPDATE_TIME;
    }

    public void setUPDATE_TIME(String UPDATE_TIME) {
        this.UPDATE_TIME = UPDATE_TIME;
    }

    public String getETL_DATE() {
        return ETL_DATE;
    }

    public void setETL_DATE(String ETL_DATE) {
        this.ETL_DATE = ETL_DATE;
    }
}


