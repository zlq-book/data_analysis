package com.travelsky.dataplatform.dwd;

import com.travelsky.dataplatform.constans.Constants;

public class SysRegisterToUserDimTransTest {
    public static void main(String[] args) {
        String tid= "++/EFd7LkTkaJ8rQUY2/WJqQRCP9P/WWSXtIOelsUHo=";
        String s="SELECT \n" +
                "CN_NAME_SOURCE\n" +
                ",EN_NAME_SOURCE\n" +
                ",PROVINCE_SOURCE\n" +
                ",SYS_REGISTER_ID_SOURCE\n" +
                ",CITY_SOURCE\n" +
                ",NATIONALITY_SOURCE\n" +
                ",EMPLOYER_SOURCE\n" +
                ",MOBILE_PHONE_SOURCE\n" +
                ",EMAIL_SOURCE\n" +
                ",ID_CARD_SOURCE\n" +
                ",PASSPORT_SOURCE\n" +
                ",SEAMAN_ID_SOURCE\n" +
                ",ALIEN_PERMIT_SOURCE\n" +
                ",DIPLOMATIC_STAFF_CERTIFICATE_SOURCE\n" +
                ",PERMANENT_RESIDENT_ID_SOURCE\n" +
                ",CIVILIAN_STAFF_ID_SOURCE\n" +
                ",STAFF_ID_SOURCE\n" +
                ",OFFICER_ID_CARD_SOURCE\n" +
                ",ARMED_POLICE_OFFICER_SOURCE\n" +
                ",ARMED_POLICE_SOLDIER_SOURCE\n" +
                ",CIVILIAN_OFFICIAL_ID_SOURCE\n" +
                ",CONSCRIPT_SOLDIER_ID_SOURCE\n" +
                ",NON_COMMISSIONED_OFFICER_ID_SOURCE\n" +
                ",HK_MACAO_RESIDENT_PERMIT_SOURCE\n" +
                ",TAIWAN_RESIDENT_TRAVEL_PERMIT_SOURCE\n" +
                ",FREQUENT_TRAVELER_CARDNO_SOURCE\n" +
                ",FT_REGISTER_TIME_SOURCE\n" +
                ",ACCEPT_SMS_MARKETING_SOURCE\n" +
                ",ACCEPT_EMAIL_MARKETING_SOURCE\n" +
                ",PARENT_FT_CARDNO_SOURCE\n" +
                ",SYS_REGISTER_ID\n" +
                " FROM "+Constants.DIM_DB+".T_DIM_USER_DIM_SUMMARY T1\n" +
                " LEFT JOIN "+Constants.DIM_DB+".T_DIM_USER_DIM T2 ON T1.PK_ID=T2.PK_ID\n" +
                " WHERE T1.PK_ID='"+tid+"'";
        System.out.println(s);
    }
}
