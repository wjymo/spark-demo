package com.wjy.util;

import com.ggstar.util.ip.IpHelper;

public class IpUtils {
    public static String getCity(String ip){
        return IpHelper.findRegionByIp(ip);
    }
}
