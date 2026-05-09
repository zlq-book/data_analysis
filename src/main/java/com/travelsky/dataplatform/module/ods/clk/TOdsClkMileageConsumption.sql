DROP TABLE IF EXISTS T_ODS_CLK_MILEAGE_CONSUMPTION;

CREATE TABLE IF NOT EXISTS T_ODS_CLK_MILEAGE_CONSUMPTION (
    ID BIGINT NOT NULL COMMENT '主键ID',
    `DATE` DATE REPLACE NULL COMMENT '日期',
    AIRLINE VARCHAR(500) REPLACE NULL COMMENT '航司',
    FREE_TICKET DECIMAL(20,2) REPLACE NULL COMMENT '免票',
    CABIN_UPGRADE DECIMAL(20,2) REPLACE NULL COMMENT '升舱',
    NON_FLIGHT DECIMAL(20,2) REPLACE NULL COMMENT '非航',
    TOTAL DECIMAL(20,2) REPLACE NULL COMMENT '总',
    ETL_CREATE_TIME DATETIME(3) REPLACE NULL COMMENT '数据入仓时间',
    ETL_UPDATE_TIME DATETIME(3) REPLACE NULL COMMENT '数据在数仓更新时间',
    ETL_DATE DATE REPLACE NULL COMMENT '数据ETL日期'
)
ENGINE=OLAP
AGGREGATE KEY(`ID`)
COMMENT '国航系里程消费明细表'
DISTRIBUTED BY HASH(`ID`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V1",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);