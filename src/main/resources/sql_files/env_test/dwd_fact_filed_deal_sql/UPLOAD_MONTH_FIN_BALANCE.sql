-- 1. 设置会话变量 (在执行 INSERT 语句前执行)
--
-- SET batch_size = 4096;
--
-- 财务运输数据(结算数据)UPLOAD_MONTH_FIN_BALANCE-成行-航段级事实表
-- SET @ETL_DATE = '2025-04-30';
INSERT INTO DWD_TEST.T_DWD_DEPART_SEG_FACT(
PK_ID,TRANSPORT_CURRENCY,SEG_TRANSPORT_TIK_PRICE,
SEG_TRANSPORT_AGENCY_FEE,SEG_TRANSPORT_NET_AMOUNT,AGENT,SYSTEM_CREATETIME)
SELECT
             -- 主键：TICKET_AIRLINE+TICEKET_NUM+ORIG+DEP_DATE
             CONCAT(
                     COALESCE(TICKET_AIRLINE, ''),
                     COALESCE(TICEKET_NUM, ''),
                     COALESCE(ORIG, ''),
                     COALESCE(DEP_DATE, '')
                 )     AS PK_ID,
             -- 运输币种
             'CNY'    AS TRANSPORT_CURRENCY,
             -- 航段运输票面价格CNY
             NET_INCOME   AS SEG_TRANSPORT_TIK_PRICE,
             -- 航段运输代理费CNY
             AGENT_AMT  AS SEG_TRANSPORT_AGENCY_FEE,
             -- 航段运输票面净额CNY
             NET_INCOME-NVL(AGENT_AMT,0)  AS SEG_TRANSPORT_NET_AMOUNT,
             -- 代理人名称
             AGENT     AS AGENT,
             -- 系统创建时间
             NOW()     AS SYSTEM_CREATETIME
         FROM ODS_TEST.T_ODS_SC_UPLOAD_MONTH_FIN_BALANCE
         WHERE ETL_DATE >= @ETL_DATE
--            AND ETL_DATE <= @ETL_DATE
-- 事务提交
-- commit;
