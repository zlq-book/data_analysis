CREATE TABLE IF NOT EXISTS T_ODS_CLK_QUALIFIED_NO_REGISTRATION (
    ID BIGINT NOT NULL COMMENT '主键ID',
    ACTIVITY_CHINESE_NAME VARCHAR(500) REPLACE NULL COMMENT '活动中文名称',
    ACTIVITY_CODE VARCHAR(500) REPLACE NULL COMMENT '活动code',
    MEMBER_CARD_NUMBER VARCHAR(500) REPLACE NULL COMMENT '常客卡号',
    ETL_CREATE_TIME DATETIME(3) REPLACE NULL COMMENT '数据入仓时间',
    ETL_UPDATE_TIME DATETIME(3) REPLACE NULL COMMENT '数据在数仓更新时间',
    ETL_DATE DATE REPLACE NULL COMMENT '数据ETL日期'
)
ENGINE=OLAP
AGGREGATE KEY(`ID`)
COMMENT '达标人群(无需报名)数据表'
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