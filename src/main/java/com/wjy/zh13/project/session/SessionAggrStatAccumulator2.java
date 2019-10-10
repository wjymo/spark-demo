package com.wjy.zh13.project.session;

import com.wjy.zh13.project.constant.Constants;
import com.wjy.zh13.project.utils.MyStringUtils;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.util.AccumulatorV2;

/**
 * session聚合统计Accumulator
 * 
 * 大家可以看到
 * 其实使用自己定义的一些数据格式，比如String，甚至说，我们可以自己定义model，自己定义的类（必须可序列化）
 * 然后呢，可以基于这种特殊的数据格式，可以实现自己复杂的分布式的计算逻辑
 * 各个task，分布式在运行，可以根据你的需求，task给Accumulator传入不同的值
 * 根据不同的值，去做复杂的逻辑
 * 
 * Spark Core里面很实用的高端技术
 * 
 * @author Administrator
 *
 */
public class SessionAggrStatAccumulator2 extends AccumulatorV2<String,String> {

	private static final long serialVersionUID = 6311074555136039130L;

	private String result=Constants.SESSION_COUNT + "=0|"
			+ Constants.TIME_PERIOD_1s_3s + "=0|"
			+ Constants.TIME_PERIOD_4s_6s + "=0|"
			+ Constants.TIME_PERIOD_7s_9s + "=0|"
			+ Constants.TIME_PERIOD_10s_30s + "=0|"
			+ Constants.TIME_PERIOD_30s_60s + "=0|"
			+ Constants.TIME_PERIOD_1m_3m + "=0|"
			+ Constants.TIME_PERIOD_3m_10m + "=0|"
			+ Constants.TIME_PERIOD_10m_30m + "=0|"
			+ Constants.TIME_PERIOD_30m + "=0|"
			+ Constants.STEP_PERIOD_1_3 + "=0|"
			+ Constants.STEP_PERIOD_4_6 + "=0|"
			+ Constants.STEP_PERIOD_7_9 + "=0|"
			+ Constants.STEP_PERIOD_10_30 + "=0|"
			+ Constants.STEP_PERIOD_30_60 + "=0|"
			+ Constants.STEP_PERIOD_60 + "=0";
	
	/**
	 * session统计计算逻辑
	 * @param str 连接串
	 * @param key 范围区间
	 * @return 更新以后的连接串
	 */
	private String add(String str, String key) {
		// 校验：v1为空的话，直接返回v2
		if(MyStringUtils.isEmpty(str)) {
			return key;
		}
		
		// 使用MyStringUtils工具类，从v1中，提取v2对应的值，并累加1
		String oldValue = MyStringUtils.getFieldFromConcatString(str, "\\|", key);
		if(oldValue != null) {
			// 将范围区间原有的值，累加1
			int newValue = Integer.valueOf(oldValue) + 1;
			// 使用MyStringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
			return MyStringUtils.setFieldInConcatString(str, "\\|", key, String.valueOf(newValue));
		}
		
		return str;
	}

    private String addVal(String str, String key,Integer value) {
        // 校验：v1为空的话，直接返回v2
        if(MyStringUtils.isEmpty(str)) {
            return key;
        }

        // 使用MyStringUtils工具类，从v1中，提取v2对应的值，并累加1
        String oldValue = MyStringUtils.getFieldFromConcatString(str, "\\|", key);
        if(oldValue != null) {
            // 将范围区间原有的值，累加1
            int newValue = Integer.valueOf(oldValue) + value;
            // 使用MyStringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
            return MyStringUtils.setFieldInConcatString(str, "\\|", key, String.valueOf(newValue));
        }

        return str;
    }

	@Override
	public boolean isZero() {
		String s = Constants.SESSION_COUNT + "=0|"
				+ Constants.TIME_PERIOD_1s_3s + "=0|"
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|"
				+ Constants.STEP_PERIOD_1_3 + "=0|"
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0";
		return s.equals(result);
	}

	@Override
	public AccumulatorV2<String, String> copy() {
		return new SessionAggrStatAccumulator2();
	}

	@Override
	public void reset() {
		result=Constants.SESSION_COUNT + "=0|"
				+ Constants.TIME_PERIOD_1s_3s + "=0|"
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|"
				+ Constants.STEP_PERIOD_1_3 + "=0|"
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}

	@Override
	public void add(String key) {
		result=add(result,key);
	}

	@Override
	public void merge(AccumulatorV2<String, String> other) {
        String[] split = other.value().split("\\|");
        for (String s : split) {
            String field = s.split("=")[0];
            String value = s.split("=")[1];
            result=addVal(result,field,Integer.valueOf(value));
        }
	}

	@Override
	public String value() {
		return result;
	}
}
