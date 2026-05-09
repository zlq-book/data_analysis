package com.travelsky.trp.usercenter.data.analysis.utils;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.BookingSegFactModel;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;

/**
 * 异步查询处理
 * @author TS.SHA.fuhuazhang
 * @date 2025/8/4  15:48
 */
public class DorisAsyncNameQueryFunction extends RichAsyncFunction<BookingSegFactModel, BookingSegFactModel> {
    private static final Logger logger = LoggerFactory.getLogger(DorisAsyncNameQueryFunction.class);

    private transient Connection connection;
    private transient ExecutorService executor;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化线程池（用于异步查询）
        executor = Executors.newFixedThreadPool(10);

        // 初始化 Doris 连接（Doris 支持 MySQL 协议）
        StringBuffer urlBuffer = new StringBuffer();
        urlBuffer.append("jdbc:mysql://")
                 .append(Constants.DORIS_IP)
                 .append(":")
                 .append(Constants.DORIS_PORT)
                 .append("/")
                 .append(Constants.DWD_DB);
        connection = DriverManager.getConnection(
                urlBuffer.toString(),
                Constants.DWD_USER,
                Constants.DWD_PWD
        );

    }

    @Override
    public void asyncInvoke(
            BookingSegFactModel newModel,
            ResultFuture<BookingSegFactModel> resultFuture
    ) throws Exception {
        String oldPkId = newModel.getOldPkId();

        CompletableFuture.supplyAsync(() -> {
            try (PreparedStatement stmt = connection.prepareStatement(
                    "SELECT * FROM T_DWD_BOOKING_SEG_FACT WHERE pk_id = ?"
            )) {

                // 设置查询参数
                stmt.setString(1, oldPkId);
                stmt.setQueryTimeout(10); // 10秒超时

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        // 将ResultSet转换为旧数据对象
                        BookingSegFactModel oldModel = mapResultSetToModel(rs);
                        // 合并字段（排除主键和姓名）
                        mergeModels(oldModel, newModel);
                        return newModel;
                    }
                }
            } catch (SQLException e) {
                logger.error("Doris查询失败: pkId={}", oldPkId, e);
            }
            return newModel; // 查询失败或数据不存在时返回原对象
        }, executor).thenAccept(result -> {
            resultFuture.complete(Collections.singleton(result));
        });
    }

    @Override
    public void timeout(BookingSegFactModel input, ResultFuture<BookingSegFactModel> resultFuture) {
        logger.warn("查询超时: pkId={}", input.getOldPkId());
        resultFuture.complete(Collections.singleton(input)); // 超时返回原数据
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.shutdown();
        }
        if (connection != null) {
            connection.close();
        }
    }

    private BookingSegFactModel mapResultSetToModel(ResultSet rs) throws SQLException {
        BookingSegFactModel model = new BookingSegFactModel();
        // 基本信息映射
        model.setOldPkId(rs.getString("PK_ID"));
        model.setPnrNumber(rs.getString("PNR_NUMBER"));
        model.setFkBookingDate(rs.getString("FK_BOOKING_DATE"));
        model.setFkBookingTime(rs.getString("FK_BOOKING_TIME"));
        model.setFkDepairport(rs.getString("FK_DEPAIRPORT"));
        model.setFkArriairport(rs.getString("FK_ARRIAIRPORT"));
        model.setFkBookingSeg(rs.getString("FK_BOOKING_SEG"));
        model.setFkSegDate(rs.getString("FK_SEG_DATE"));
        model.setFkSegTime(rs.getString("FK_SEG_TIME"));
        model.setPassengerType(rs.getString("PASSENGER_TYPE"));
        model.setEnLastName(rs.getString("EN_LAST_NAME"));
        model.setEnFirstName(rs.getString("EN_FIRST_NAME"));
        model.setCnName(rs.getString("CN_NAME"));
        model.setCertType(rs.getString("CERT_TYPE"));
        model.setCertNumber(rs.getString("CERT_NUMBER"));
        model.setPassengerAge(getIntegerValue(rs,"PASSENGER_AGE"));
        model.setFkPassengerUserTid(rs.getString("FK_PASSENGER_USER_TID"));
        model.setVvip(rs.getString("VVIP"));
        model.setAkBookingOfficeNumber(rs.getString("AK_BOOKING_OFFICE_NUMBER"));
        model.setAkTeammark(rs.getBoolean("AK_TEAMMARK"));
        model.setAkSegStatus(rs.getString("AK_SEG_STATUS"));
        model.setAkCabin(rs.getString("AK_CABIN"));
        model.setAkSegcabin(rs.getString("AK_SEGCABIN"));
        //model.setPriceType(rs.getString("PRICE_TYPE"));
        model.setFfrf(rs.getString("FFRF"));
        model.setFfLevel(rs.getString("FF_LEVEL"));
        model.setFfAirline(rs.getString("FF_AIRLINE"));
        model.setFfAllianceLevel(rs.getString("FF_ALLIANCE_LEVEL"));
        model.setAkAdvbookDay(getIntegerValue(rs,"AK_ADVBOOK_DAY"));
        model.setAkKeyAccountCode(rs.getString("AK_KEY_ACCOUNT_CODE"));
        model.setKeyAccount(getBooleanValue( rs,"IS_KEY_ACCOUNT"));
        model.setAkPeerNumber(getIntegerValue(rs,"AK_PEER_NUMBER"));
        model.setAkPeerChd(getBooleanValue( rs,"AK_PEER_CHD"));
        model.setPeerSenior(getBooleanValue( rs,"PEER_SENIOR"));
        //model.setPeerInfant(getBooleanValue( rs,"PEER_INFANT"));
        //model.setSeasonTag(rs.getString("SEASON_TAG"));
        model.setSelfBooking(getBooleanValue( rs,"SELF_BOOKING"));
        model.setHomecomingSeg(getBooleanValue( rs,"HOMECOMING_SEG"));
        model.setSingleTravel(getBooleanValue( rs,"SINGLE_TRAVEL"));
        model.setSegCount(getIntegerValue(rs,"SEG_COUNT"));
        model.setChannelOrderId(rs.getString("CHANNEL_ORDER_ID"));
        model.setAkChannel(rs.getString("AK_CHANNEL"));
        model.setContactMobileNumber(rs.getString("CONTACT_MOBILE_NUMBER"));
        model.setContactName(rs.getString("CONTACT_NAME"));
        //model.setContactInterMobileCode(rs.getString("CONTACT_INTER_MOBILE_CODE"));
        model.setContactLandlineMobileNumber(rs.getString("CONTACT_LANDLINE_MOBILE_NUMBER"));
        //model.setContactEmail(rs.getString("CONTACT_EMAIL"));
        model.setFkBookingUserTid(rs.getString("FK_BOOKING_USER_TID"));
        model.setFkBookingUserOriginId(rs.getString("FK_BOOKING_USER_ORIGIN_ID"));
        //model.setAkBookingBrand(rs.getString("AK_BOOKING_BRAND"));
        model.setAkBookerIsPassenger(getBooleanValue( rs,"AK_BOOKER_IS_PASSENGER"));
        //model.setAkIsseckill(getBooleanValue( rs,"AK_ISSECKILL"));
        model.setDiscount(getDoubleValue(rs,"DISCOUNT"));
        model.setDataActive(getBooleanValue( rs,"DATA_ACTIVE"));
        model.setDataActiveTime(rs.getString("DATA_ACTIVE_TIME"));
        model.setSourceLastUpdatetime(rs.getString("SOURCE_LAST_UPDATETIME"));
        model.setSystemCreatetime(rs.getString("SYSTEM_CREATETIME"));
        model.setSystemLastUpdatetime(rs.getString("SYSTEM_LAST_UPDATETIME"));
        return model;
    }

    // 辅助方法：安全获取Integer值
    private Integer getIntegerValue(ResultSet rs, String columnName) throws SQLException {
        Object value = rs.getObject(columnName);
        return value != null ? rs.getInt(columnName) : null;
    }

    // 辅助方法：安全获取Double值
    private Double getDoubleValue(ResultSet rs, String columnName) throws SQLException {
        Object value = rs.getObject(columnName);
        return value != null ? rs.getDouble(columnName) : null;
    }

    // 辅助方法：安全获取Boolean值
    private Boolean getBooleanValue(ResultSet rs, String columnName) throws SQLException {
        Object value = rs.getObject(columnName);
        return value != null ? rs.getBoolean(columnName) : null;
    }

    private void mergeModels(BookingSegFactModel oldModel, BookingSegFactModel newModel) {
        try {
            String newPkId = newModel.getPkId();
            String surname = newModel.getEnLastName();
            String givenName = newModel.getEnFirstName();
            String nativeGivenName = newModel.getCnName();
            String lastUpdateTime = newModel.getSystemLastUpdatetime();

            // 使用BeanUtils复制旧数据到新数据对象（排除关键字段）
            BeanUtils.copyProperties(newModel, oldModel);
            newModel.setPkId(newPkId);
            newModel.setEnLastName(surname);
            newModel.setEnFirstName(givenName);
            newModel.setCnName(nativeGivenName);
            newModel.setSystemLastUpdatetime(lastUpdateTime);
        } catch (Exception e) {
            logger.error("合并模型失败", e);
        }
    }
}
