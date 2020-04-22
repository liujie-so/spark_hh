package com.ky.util;

import java.util.Map;

import org.apache.commons.lang.ArrayUtils;

import com.google.common.collect.Maps;

public class MapUtils extends org.apache.commons.collections.MapUtils {
	/**
	 * 创建Map，传入一个数据，偶数下标为KEY、奇数下标为VALUE
	 * @param obj
	 * @return
	 * @author liu.jie
	 * @since 2018年10月23日
	 */
	public static Map<String, Object> create(Object... obj) {
		return creator(obj);
	}
	
	/**
	 * 新建Map对象，同一种类型的KEY/VALUE值
	 * @author liu.jie
	 * @since 2019年4月17日
	 * @param array
	 * @return
	 */
	@SafeVarargs
	public static <V> Map<String, V> creator(V... array) {
		Map<String, V> map = Maps.newHashMap();
		if(ArrayUtils.isEmpty(array) || array.length%2 != 0) {
			return map;
		}
		for (int i = 0; i < array.length; i++) {
			if(i%2 == 0) {
				map.put(array[i].toString(), array[i + 1]);
			}
		}
		return map;
	}
}
