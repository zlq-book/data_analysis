DROP TABLE IF EXISTS T_ODS_CLK_VIP_MEMBER_TOTAL;

CREATE TABLE IF NOT EXISTS T_ODS_CLK_VIP_MEMBER_TOTAL (
    ID BIGINT NOT NULL COMMENT '主键ID',
    `DATE` DATE REPLACE NULL COMMENT '日期',
    NAME VARCHAR(500) REPLACE NULL COMMENT '名称',
    FINAL_WHITE DECIMAL(20,2) REPLACE NULL COMMENT '终白',
    PLATINUM DECIMAL(20,2) REPLACE NULL COMMENT '白金',
    GOLD DECIMAL(20,2) REPLACE NULL COMMENT '金',
    SILVER DECIMAL(20,2) REPLACE NULL COMMENT '银',
    TOTAL_BY_ATTRIBUTES DECIMAL(20,2) REPLACE NULL COMMENT '各属性总量',
    ETL_CREATE_TIME DATETIME(3) REPLACE NULL COMMENT '数据入仓时间',
    ETL_UPDATE_TIME DATETIME(3) REPLACE NULL COMMENT '数据在数仓更新时间',
    ETL_DATE DATE REPLACE NULL COMMENT '数据ETL日期'
)
ENGINE=OLAP
AGGREGATE KEY(`ID`)
COMMENT '国航系贵宾会员总量表'
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