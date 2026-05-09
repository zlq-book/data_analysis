package com.travelsky.trp.usercenter.data.analysis.utils.channel;

import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;
import org.apache.commons.lang3.StringUtils;

public class ChannelDictService {

    private static volatile ChannelDictCache cache;

    private static void ensureLoaded() {
        if (cache == null) {
            synchronized (ChannelDictService.class) {
                if (cache == null) {
                    ChannelDictCache c = new ChannelDictCache();
                    try {
                        c.load();
                    } catch (Exception e) {
                        // ignore load error, keep cache null
                    }
                    cache = c;
                }
            }
        }
    }

    public static ChannelInfo matchByOperateDept(String operateDept) {
        return matchByOperateDept(operateDept, null);
    }

    public static ChannelInfo matchByOperateDept(String operateDept, String ffpDeptFlag) {
        if (StringUtils.isBlank(operateDept)) {
            return null;
        }
        ensureLoaded();
        if (cache == null) {
            return null;
        }
        return cache.matchByOperateDept(operateDept, ffpDeptFlag);
    }
}


