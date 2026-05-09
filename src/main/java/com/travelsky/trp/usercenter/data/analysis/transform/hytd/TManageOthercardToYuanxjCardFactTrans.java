package com.travelsky.trp.usercenter.data.analysis.transform.hytd;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.dataplatform.utils.DateTimeUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.CertDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.YuanxjCardFactModal;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kuangaihua
 * @date 2025/8/6 9:08
 */
public class TManageOthercardToYuanxjCardFactTrans {
    private static final Logger logger = LoggerFactory.getLogger(TManageOthercardToYuanxjCardFactTrans.class);

    public static void result(DataStream<JSONObject> sourceStream) {
        DataStream<YuanxjCardFactModal> yuanxjCardFactModalDataStream = sourceStream.map(row -> {
            YuanxjCardFactModal yuanxjCardFactModal = new YuanxjCardFactModal();
            String id = row.getString("id" );
            String crmCardno = row.getString("crmCardno" );
            String cardNo = row.getString("cardNo" );
            String name = row.getString("name" );
            String nameEn = row.getString("nameEn" );
            String certType = row.getString("certType" );
            String certNo = row.getString("certNo" );
            String endTime = row.getString("endTime" );
            String tid = row.getString("TID" );
            String currentDateTime = DateTimeUtils.getCurrentDateTime();
            yuanxjCardFactModal.setPkId( id);
            yuanxjCardFactModal.setCrmCard( crmCardno);
            yuanxjCardFactModal.setYuanjiangCard( cardNo);
            yuanxjCardFactModal.setCnName( name);
            yuanxjCardFactModal.setEnName( nameEn);
            yuanxjCardFactModal.setCertifiType( certType);
            yuanxjCardFactModal.setCertifiNumber( certNo);
            yuanxjCardFactModal.setCardExpiredate( endTime);
            yuanxjCardFactModal.setApplicantsTid( tid);
            yuanxjCardFactModal.setTotalCount( 1);
            yuanxjCardFactModal.setSystemCreatetime( currentDateTime);
            yuanxjCardFactModal.setSystemLastUpdatetime( currentDateTime);
            return yuanxjCardFactModal;
        }).filter(row -> row != null);
        // 创建 Doris Sink
        DorisSink<YuanxjCardFactModal> yuanxjCardFactModalDorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_YUANXJ_CARD_FACT");
        yuanxjCardFactModalDataStream.sinkTo(yuanxjCardFactModalDorisSink);
    }
}
