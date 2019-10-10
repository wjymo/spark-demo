package com.wjy.util;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 访问日志转换(输入==>输出)工具类
 */
public class AccessConvertUtil {
    private  static List<String> cityList=null;
    private  static List<String> cmsTypeList=null;

    static {
        cityList= Arrays.asList("湖北","印度","乌鲁木齐","乌兹别克斯坦","吉尔吉斯斯坦","车臣","香港","日本","日内瓦","赞比亚");
        cmsTypeList=Arrays.asList("课程","手记");
    }
    /**
     * 根据输入的每一行信息转换成输出的样式
     * @param log  输入的每一行记录信息
     */
    public static Row parseLog(String log)  {
        try{
            String[] splits = log.split("\t");

            String url = splits[1];
            Long traffic = Long.valueOf((String)splits[2]);
            String ip = splits[3];

//            String domain = "http://www.imooc.com/";
//            String cms = url.substring(url.indexOf(domain) + domain.length());
//            String[] cmsTypeId = cms.split("/");

            String cmsType = cmsTypeList.get(RandomUtils.nextInt(0,2));
            Long cmsId = RandomUtils.nextLong(10,30);
//            if(cmsTypeId.length > 1) {
//                cmsType = cmsTypeId[0];
//                cmsId = Long.valueOf((String)cmsTypeId[1]);
//            }

            String city = IpUtils.getCity(ip);
            if(StringUtils.equals("全球",city)){
                int i = RandomUtils.nextInt(0,10);
                city = (String)cityList.get(i);
            }
            String time = splits[0];
            String day = time.substring(0,10).replaceAll("-","");

            //这个row里面的字段要和struct中的字段对应上

            return RowFactory.create(url, cmsType, cmsId, traffic, ip, city, time, day);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        int i = RandomUtils.nextInt(0,10);
        System.out.println(i);
    }
}
