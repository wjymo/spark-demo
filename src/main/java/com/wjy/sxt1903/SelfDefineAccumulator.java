package com.wjy.sxt1903;

import com.wjy.zh13.project.constant.Constants;
import com.wjy.zh13.project.utils.MyStringUtils;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.util.AccumulatorV2;

public class SelfDefineAccumulator extends AccumulatorV2<String, String> {

    String returnResult = "";
    @Override
    public boolean isZero() {
        //normalMonitorCount=0|normalCameraCount=0|abnormalMonitorCount=0|abnormalCameraCount=0|abnormalMonitorCameraInfos=
        return "normalMonitorCount=0|normalCameraCount=0|abnormalMonitorCount=0|abnormalCameraCount=0|abnormalMonitorCameraInfos= "
                .equals(returnResult);
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        SelfDefineAccumulator acc  = new SelfDefineAccumulator();
        acc.returnResult = this.returnResult;
        return acc;
    }

    @Override
    public void reset() {
        //normalMonitorCount=0|normalCameraCount=0|abnormalMonitorCount=0|abnormalCameraCount=0|abnormalMonitorCameraInfos=
        returnResult = Constants.FIELD_NORMAL_MONITOR_COUNT+"=0|"
                + Constants.FIELD_NORMAL_CAMERA_COUNT+"=0|"
                + Constants.FIELD_ABNORMAL_MONITOR_COUNT+"=0|"
                + Constants.FIELD_ABNORMAL_CAMERA_COUNT+"=0|"
                + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS+"= ";
    }

    @Override
    public void add(String v) {
        returnResult = myAdd(returnResult, v);
    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
        SelfDefineAccumulator selfDefineAccumulator = (SelfDefineAccumulator) other;
        returnResult=myAdd(returnResult,selfDefineAccumulator.returnResult);
    }

    @Override
    public String value() {
        return returnResult;
    }

    private String myAdd(String v1,String v2){
        if(StringUtils.isEmpty(v1)){
            throw new RuntimeException("累加器的值未初始化!");
        }
        String[] split = v2.split("\\|");
        for (String s : split) {
            String[] fieldAndValArr = s.split("=");
            String field = fieldAndValArr[0];
            String value = fieldAndValArr[1];
            String oldValue = MyStringUtils.getFieldFromConcatString(v1, "\\|", field);
            if(oldValue != null){
                //只有这个字段是string，所以单独拿出来
                if(StringUtils.equals(Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS,field)){
                    v1 = MyStringUtils.setFieldInConcatString(v1, "\\|", field, oldValue + "~" + value);
                }else {
                    int newValue = Integer.valueOf(oldValue) + Integer.valueOf(value);
                    v1=MyStringUtils.setFieldInConcatString(v1,"\\|",field,String.valueOf(newValue));
                }
            }else {
                throw new RuntimeException("累加器的值中字段:"+field+"为空!");
            }
        }
        return v1;
    }
}
