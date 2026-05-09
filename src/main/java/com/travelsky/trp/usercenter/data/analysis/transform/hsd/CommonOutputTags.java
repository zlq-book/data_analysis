package com.travelsky.trp.usercenter.data.analysis.transform.hsd;

import com.travelsky.trp.usercenter.data.analysis.model.dim.*;
import org.apache.flink.util.OutputTag;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.*;

/**
 * 通用的OutputTag管理器，供所有Trans类使用
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/28  15:38
 */
public class CommonOutputTags {

    // TickingSegFactModel 相关标签
    public static final OutputTag<TickingSegFactModel> TICKING_SEG_TAG =
            new OutputTag<TickingSegFactModel>("ticking-seg") {};

    // TickingTicFactModel 相关标签
    public static final OutputTag<TickingTicFactModel> TICKING_TIC_TAG =
            new OutputTag<TickingTicFactModel>("ticking-tic") {};

    // DatechangeSegFactModel 相关标签
    public static final OutputTag<DatechangeSegFactModel> DATECHANGE_SEG_TAG =
            new OutputTag<DatechangeSegFactModel>("datechange-seg") {};

    // RefundSegFactModel 相关标签
    public static final OutputTag<RefundSegFactModel> REFUND_SEG_TAG =
            new OutputTag<RefundSegFactModel>("refund-seg") {};

    // DepartSegFactModel 相关标签
    public static final OutputTag<DepartSegFactModel> DEPART_SEG_TAG =
            new OutputTag<DepartSegFactModel>("depart-seg") {};

    // SeatTikFactModel 相关标签
    public static final OutputTag<SeatTikFactModel> SEAT_TIK_TAG =
            new OutputTag<SeatTikFactModel>("seat-tik") {};

     // SeatRefundFactModel 相关标签
    public static final OutputTag<SeatRefundFactModel> SEAT_REFUND_TAG =
            new OutputTag<SeatRefundFactModel>("seat-refund") {};

    // BaggageTikFactModel 相关标签
    public static final OutputTag<BaggageTikFactModel> BAGGAGE_TIK_TAG =
            new OutputTag<BaggageTikFactModel>("baggage-tik") {};

    // BaggageRefundFactModel 相关标签
    public static final OutputTag<BaggageRefundFactModel> BAGGAGE_REFUND_TAG =
            new OutputTag<BaggageRefundFactModel>("baggage-refund") {};

    // MealTikFactModel 相关标签
    public static final OutputTag<MealTikFactModel> MEAL_TIK_TAG =
            new OutputTag<MealTikFactModel>("meal-tik") {};

    // MealRefundFactModel 相关标签
    public static final OutputTag<MealRefundFactModel> MEAL_REFUND_TAG =
            new OutputTag<MealRefundFactModel>("meal-refund") {};

    // UpgrTikFactModel 相关标签
    public static final OutputTag<UpgrTikFactModel> UPGR_TIK_TAG =
            new OutputTag<UpgrTikFactModel>("upgr-tik") {};

     // UpgrRefundFactModel 相关标签
    public static final OutputTag<UpgrRefundFactModel> UPGR_REFUND_TAG =
            new OutputTag<UpgrRefundFactModel>("upgr-refund") {};

    // ExcessBaggageTikFactModel 相关标签
    public static final OutputTag<ExcessBaggageTikFactModel> EXCESS_BAGGAGE_TIK_TAG =
            new OutputTag<ExcessBaggageTikFactModel>("excess-baggage-tik") {};

    // UmchildTikFactModel 相关标签
    public static final OutputTag<UmchildTikFactModel> UMCHILD_TIK_TAG =
            new OutputTag<UmchildTikFactModel>("umchild-tik") {};

    // ChargefeeTikFactModal 相关标签
    public static final OutputTag<ChargefeeTikFactModal> CHARGEFEE_TIK_TAG =
            new OutputTag<ChargefeeTikFactModal>("chargefee-tik") {};

    // BookingPnrFactModel 相关标签
    public static final OutputTag<BookingPnrFactModel> BOOKING_PNR_TAG =
            new OutputTag<BookingPnrFactModel>("booking-pnr") {};

    // BookingSegFactModel 相关标签
    public static final OutputTag<BookingSegFactModel> BOOKING_SEG_TAG =
            new OutputTag<BookingSegFactModel>("booking-seg") {};

    // CheckinSegFactModel 相关标签
    public static final OutputTag<CheckinSegFactModel> CHECKIN_SEG_TAG =
            new OutputTag<CheckinSegFactModel>("checkin-seg") {};

    // UserDimModel 相关标签
    public static final OutputTag<UserDimModel> USER_DIM_TAG =
            new OutputTag<UserDimModel>("user-dim") {};

    // UserDimSummaryModel 相关标签
    public static final OutputTag<UserDimSummaryModel> USER_DIM_SUMMARY_TAG =
            new OutputTag<UserDimSummaryModel>("user-dim-summary") {};

    // CertDimModel 相关标签
    public static final OutputTag<CertDimModel> CERT_DIM_TAG =
            new OutputTag<CertDimModel>("cert-dim") {};

    // MobileDimModel 相关标签
    public static final OutputTag<MobileDimModel> MOBILE_DIM_TAG =
            new OutputTag<MobileDimModel>("mobile-dim") {};

    // CustomDimModel 相关标签
    public static final OutputTag<CustomDimModel> CUSTOM_DIM_TAG =
            new OutputTag<CustomDimModel>("custom-dim") {};

    // FfpDimModel 相关标签
    public static final OutputTag<FfpDimModel> FFP_DIM_TAG =
            new OutputTag<FfpDimModel>("ffp-dim") {};

    // SegDimModel 相关标签
    public static final OutputTag<SegDimModel> SEG_DIM_TAG =
            new OutputTag<SegDimModel>("seg-dim") {};


    // HsdProcessDataModel 相关标签
    public static final OutputTag<HsdProcessDataModel> HSD_PROCESS_DATA_TAG =
            new OutputTag<HsdProcessDataModel>("hsd-process-data") {};

}
