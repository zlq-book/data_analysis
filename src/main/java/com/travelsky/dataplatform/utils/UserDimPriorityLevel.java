package com.travelsky.dataplatform.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author kuangaihua
 * @date 2025/8/6 14:28
 */
public class UserDimPriorityLevel {
    /**
     * 数据来源做统一
     * 实时高频：HSD
     * 机上注册：CLK
     * 鲁雁行：LYX
     * 鲁雁行实名：LYXSM
     * 鲁雁行实名认证标识是已认证的且认证方式不为LIP认证的手机号，或鲁雁行认证方式为LIP认证同时三要素认证结果是匹配的手机号：LYXSMS
     * 直销用户：ZXYH
     * 直销用户实名：ZXYHSM
     * 直销实名认证且认证方式为三要素或四要素的客户手机号：ZXYHSMS
     * 会员天地：HYTD
     * 历史常客：LSCK
     * 掌尚飞（抖音次卡）：ZSF
     * 历史高频：LYGJ
     * 高端旅客: GDLK
     * TRP乘机人:TRP
     * SPNR预订人:ZXYH
     * 大客户差旅系统军人:DKHCL
     * 身份证：SFZ
     * 鲁雁管家投诉客户：LYGJ
     * 升舱整合平台机上升舱乘机人证件：JSSC
     * 白屏票务服务系统：BPPW
     * 企微小山：QWXS
     * 官网非注册用户手机号:GW
     * 升舱整合平台预约升舱（券）预订人:预约升舱
     * 23年12月31日前注册的常客：LSZC
     * 抖音次卡购买人:CKGM
     * * *
     */

    //证件号
    public static boolean credential(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("HSD", "ZXYHSM", "LYXSM", "HYTD", "CLK", "LSCK", "ZSF");
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        if (firstLevel.contains(newSource)) newLevel = 0;
        List<String> secondLevel = Arrays.asList("LYX");
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (secondLevel.contains(newSource)) newLevel = 1;
        List<String> thirdLevel = Arrays.asList("ZXYH");
        if (thirdLevel.contains(oldSource)) oldLevel = 2;
        if (thirdLevel.contains(newSource)) newLevel = 2;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    //常客卡号
    public static boolean frequentTravelerCardno(String oldSource, String newSource) {

        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("CLK", "LSCK", "HYTD");
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        if (firstLevel.contains(newSource)) newLevel = 0;
        List<String> secondLevel = Arrays.asList("HSD");
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (secondLevel.contains(newSource)) newLevel = 1;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    //手机号
    public static boolean mobilePhone(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("ZXYHSMS", "LYXSMS");
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        if (firstLevel.contains(newSource)) newLevel = 0;
        List<String> secondLevel = Arrays.asList("GDLK");
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (secondLevel.contains(newSource)) newLevel = 1;
        List<String> thirdLevel = Arrays.asList("HYTD", "CLK");
        if (thirdLevel.contains(oldSource)) oldLevel = 2;
        if (thirdLevel.contains(newSource)) newLevel = 2;
        List<String> fourthLevel = Arrays.asList("ZXYHSM", "LYXSM");
        if (fourthLevel.contains(oldSource)) oldLevel = 3;
        if (fourthLevel.contains(newSource)) newLevel = 3;
        List<String> fifthLevel = Arrays.asList("LSCK");
        if (fifthLevel.contains(oldSource)) oldLevel = 4;
        if (fifthLevel.contains(newSource)) newLevel = 4;
        List<String> sixthLevel = Arrays.asList("ZXYH", "LYX");
        if (sixthLevel.contains(oldSource)) oldLevel = 5;
        if (sixthLevel.contains(newSource)) newLevel = 5;
        List<String> seventhLevel = Arrays.asList("ZSF");
        if (seventhLevel.contains(oldSource)) oldLevel = 6;
        if (seventhLevel.contains(newSource)) newLevel = 6;
        List<String> eighthLevel = Arrays.asList("HSD", "LYGJ");
        if (eighthLevel.contains(oldSource)) oldLevel = 7;
        if (eighthLevel.contains(newSource)) newLevel = 7;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    //工作单位
    public static boolean employer(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("GDLV");
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        if (firstLevel.contains(newSource)) newLevel = 0;
        List<String> secondLevel = Arrays.asList("HYTD");
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (secondLevel.contains(newSource)) newLevel = 1;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    //邮箱
    public static boolean email(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("ZXYH");
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        if (firstLevel.contains(newSource)) newLevel = 0;
        List<String> secondLevel = Arrays.asList("CLK");
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (secondLevel.contains(newSource)) newLevel = 1;
        List<String> thirdLevel = Arrays.asList("HYTD");
        if (thirdLevel.contains(oldSource)) oldLevel = 2;
        if (thirdLevel.contains(newSource)) newLevel = 2;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    //国籍
    public static boolean nationality(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("SPNR", "HSD");
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        if (firstLevel.contains(newSource)) newLevel = 0;
        List<String> secondLevel = Arrays.asList("LYX");
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (secondLevel.contains(newSource)) newLevel = 1;
        List<String> thirdLevel = Arrays.asList("HYTD");
        if (thirdLevel.contains(oldSource)) oldLevel = 2;
        if (thirdLevel.contains(newSource)) newLevel = 2;
        List<String> fourthLevel = Arrays.asList("ZXYH");
        if (fourthLevel.contains(oldSource)) oldLevel = 3;
        if (fourthLevel.contains(newSource)) newLevel = 3;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    //城市
    public static boolean city(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("CLK");
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        if (firstLevel.contains(newSource)) newLevel = 0;
        List<String> secondLevel = Arrays.asList("HYTD");
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (secondLevel.contains(newSource)) newLevel = 1;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    //省份
    public static boolean province(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("CLK");
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        if (firstLevel.contains(newSource)) newLevel = 0;
        List<String> secondLevel = Arrays.asList("HYTD");
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (secondLevel.contains(newSource)) newLevel = 1;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    //中文名
    public static boolean cnName(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("HSD", "CLK", "LYXSM", "ZXYHSM", "LYGJ");
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        if (firstLevel.contains(newSource)) newLevel = 0;
        List<String> secondLevel = Arrays.asList("HYTD");
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (secondLevel.contains(newSource)) newLevel = 1;
        List<String> thirdLevel = Arrays.asList("LSCK");
        if (thirdLevel.contains(oldSource)) oldLevel = 2;
        if (thirdLevel.contains(newSource)) newLevel = 2;
        List<String> fourthLevel = Arrays.asList("ZSF");
        if (fourthLevel.contains(oldSource)) oldLevel = 3;
        if (fourthLevel.contains(newSource)) newLevel = 3;
        List<String> fifthLevel = Arrays.asList("LYX");
        if (fifthLevel.contains(oldSource)) oldLevel = 4;
        if (fifthLevel.contains(newSource)) newLevel = 4;
        List<String> sixthLevel = Arrays.asList("ZXYH");
        if (sixthLevel.contains(oldSource)) oldLevel = 5;
        if (sixthLevel.contains(newSource)) newLevel = 5;
        List<String> seventhLevel = Arrays.asList("GDLK");
        if (seventhLevel.contains(oldSource)) oldLevel = 6;
        if (seventhLevel.contains(newSource)) newLevel = 6;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    //英文名
    public static boolean enName(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("HSD", "CLK", "LYXSM", "ZXYHSM", "LYGJ");
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        if (firstLevel.contains(newSource)) newLevel = 0;
        List<String> secondLevel = Arrays.asList("HYTD");
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (secondLevel.contains(newSource)) newLevel = 1;
        List<String> thirdLevel = Arrays.asList("LSCK");
        if (thirdLevel.contains(oldSource)) oldLevel = 2;
        if (thirdLevel.contains(newSource)) newLevel = 2;
        List<String> fourthLevel = Arrays.asList("ZSF");
        if (fourthLevel.contains(oldSource)) oldLevel = 3;
        if (fourthLevel.contains(newSource)) newLevel = 3;
        List<String> fifthLevel = Arrays.asList("LYX");
        if (fifthLevel.contains(oldSource)) oldLevel = 4;
        if (fifthLevel.contains(newSource)) newLevel = 4;
        List<String> sixthLevel = Arrays.asList("ZXYH");
        if (sixthLevel.contains(oldSource)) oldLevel = 5;
        if (sixthLevel.contains(newSource)) newLevel = 5;
        List<String> seventhLevel = Arrays.asList("GDLK");
        if (seventhLevel.contains(oldSource)) oldLevel = 6;
        if (seventhLevel.contains(newSource)) newLevel = 6;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    /**
     * * 民族
     *
     * @param oldSource
     * @param newSource
     * @return
     */
    public static boolean ethnicity(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("GDLK");
        if (firstLevel.contains(newSource)) newLevel = 0;
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        List<String> secondLevel = Arrays.asList("ZXYH");
        if (secondLevel.contains(newSource)) newLevel = 1;
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    /**
     * * 用户类型
     *
     * @param oldSource
     * @param newSource
     * @return
     */
    public static boolean userType(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        //优先级0：身份证解析
        //优先级1：高频实时乘机人=高频历史乘机人=TRP乘机人（无陪儿童需要转换为儿童）
        //优先级2：鲁雁管家高端旅客
        //优先级3：直销用户
        List<String> thirdLevel = Arrays.asList("ZXYH");
        if (thirdLevel.contains(newSource)) newLevel = 3;
        if (thirdLevel.contains(oldSource)) oldLevel = 3;
        List<String> secondLevel = Arrays.asList("LYGJ");
        if (secondLevel.contains(newSource)) newLevel = 2;
        if (secondLevel.contains(oldSource)) oldLevel = 2;
        List<String> firstLevel = Arrays.asList("HSD", "TRP");
        if (firstLevel.contains(newSource)) newLevel = 1;
        if (firstLevel.contains(oldSource)) oldLevel = 1;
        // 身份证
        List<String> zeroLevel = Arrays.asList("SFZ");
        if (zeroLevel.contains(newSource)) newLevel = 0;
        if (zeroLevel.contains(oldSource)) oldLevel = 0;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }
    /**
     * 性别处理方法
     * 大原则：
     * 1、优先级高的覆盖更新优先级低的。
     * 2、相同优先级如数据不同，则用时效性更新的数据去覆盖。
     */
    public static boolean sex(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;

        // 优先级0：身份证解析
        if ("SFZ".equals(oldSource)) oldLevel = 0;
        if ("SFZ".equals(newSource)) newLevel = 0;

        // 优先级1：高频实时乘机人=高频历史乘机人=TRP乘机人=机上注册用户=鲁雁行实名认证用户=直销实名认证用户
        List<String> firstLevel = Arrays.asList("HSD", "LYGJ", "SPNR", "CLK", "LYXSMS", "ZXYHSMS");
        if (firstLevel.contains(oldSource)) oldLevel = 1;
        if (firstLevel.contains(newSource)) newLevel = 1;

        // 优先级2：会员天地用户
        if ("HYTD".equals(oldSource)) oldLevel = 2;
        if ("HYTD".equals(newSource)) newLevel = 2;

        // 优先级3：历史常客
        if ("LSCK".equals(oldSource)) oldLevel = 3;
        if ("LSCK".equals(newSource)) newLevel = 3;

        // 优先级4：抖音次卡受益人=次卡受益人
        if ("ZSF".equals(oldSource)) oldLevel = 4;
        if ("ZSF".equals(newSource)) newLevel = 4;

        // 优先级5：鲁雁行非实名认证用户
        if ("LYX".equals(oldSource)) oldLevel = 5;
        if ("LYX".equals(newSource)) newLevel = 5;

        // 优先级6：直销非实名认证用户
        if ("ZXYH".equals(oldSource)) oldLevel = 6;
        if ("ZXYH".equals(newSource)) newLevel = 6;

        // 优先级7：鲁雁管家高端旅客
        if ("GDLK".equals(oldSource)) oldLevel = 7;
        if ("GDLK".equals(newSource)) newLevel = 7;

        if (newLevel <= oldLevel) flag = true;
        return flag;
    }
    /**
     * 生日数据处理方法
     * 大原则：
     * 1、优先级高的覆盖更新优先级低的。
     * 2、相同优先级如数据不同，则用时效性更新的数据去覆盖。
     */
    public static boolean handleBirthday(String oldSource, String newSource) {
        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;

        // 优先级0：身份证解析
        if ("SFZ".equals(oldSource)) oldLevel = 0;
        if ("SFZ".equals(newSource)) newLevel = 0;

        // 优先级1：高频实时乘机人=高频历史乘机人=TRP乘机人=机上注册用户=鲁雁行实名认证用户=直销实名认证用户
        List<String> firstLevel = Arrays.asList("HSD", "LYGJ", "SPNR", "CLK", "LYXSMS", "ZXYHSMS");
        if (firstLevel.contains(oldSource)) oldLevel = 1;
        if (firstLevel.contains(newSource)) newLevel = 1;

        // 优先级2：会员天地用户
        if ("HYTD".equals(oldSource)) oldLevel = 2;
        if ("HYTD".equals(newSource)) newLevel = 2;

        // 优先级3：历史常客
        if ("LSCK".equals(oldSource)) oldLevel = 3;
        if ("LSCK".equals(newSource)) newLevel = 3;

        // 优先级4：抖音次卡受益人=次卡受益人
        if ("ZSF".equals(oldSource)) oldLevel = 4;
        if ("ZSF".equals(newSource)) newLevel = 4;

        // 优先级5：鲁雁行非实名认证用户
        if ("LYX".equals(oldSource)) oldLevel = 5;
        if ("LYX".equals(newSource)) newLevel = 5;

        // 优先级6：直销非实名认证用户
        if ("ZXYH".equals(oldSource)) oldLevel = 6;
        if ("ZXYH".equals(newSource)) newLevel = 6;

        // 优先级7：鲁雁管家高端旅客
        if ("GDLK".equals(oldSource)) oldLevel = 7;
        if ("GDLK".equals(newSource)) newLevel = 7;

        if (newLevel <= oldLevel) flag = true;
        return flag;
    }

    public static boolean blindPassenger(String oldSource, String newSource) {

        boolean flag = false;
        int oldLevel = 100;
        int newLevel = 100;
        List<String> firstLevel = Arrays.asList("HSD");
        if (firstLevel.contains(newSource)) newLevel = 0;
        if (firstLevel.contains(oldSource)) oldLevel = 0;
        List<String> secondLevel = Arrays.asList("LYGJ");
        if (secondLevel.contains(newSource)) newLevel = 1;
        if (secondLevel.contains(oldSource)) oldLevel = 1;
        if (newLevel <= oldLevel) flag = true;
        return flag;
    }
}
