package com.ky.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.ArrayUtils;

/**
 * 日期工具类
 * @author liu.jie
 * @since 2019年6月21日
 */
public class DateUtils extends org.apache.commons.lang.time.DateUtils {
	
	/** 预定义格式*/
	public static final String[] innerPatterns = { "yyyy-MM-dd HH:mm:ss",
			"yyyy-MM-dd", "yyyy/MM/dd", "yyyy/MM/dd HH:mm:ss", "yyyy年MM月dd日",
			"yyyy.MM.dd", "yyyy.MM.dd HH:mm:ss", "yyyyMMddHHmmss" };
	
	/**
	 * 转换多种格式日期
	 * @author liu.jie
	 * @since 2019年6月21日
	 * @param value
	 * @param customPatterns	自定义格式
	 * @return
	 */
	public static Date parseFullDate(Object value, String... customPatterns) {
		if(StringUtils.isBlank(value)) {
			return null;
		}
		try {
			if (value instanceof String) {
				if(ArrayUtils.isNotEmpty(customPatterns)) {
					value = parseDate(value.toString(), customPatterns);
				} else {
					value = parseDate(value.toString(), innerPatterns);
				}
				return (Date) value;
			} else if (value instanceof Date) {
				return (Date) value;
			} else if(value instanceof Long) {
				
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * 格式化日期
	 * @author liu.jie
	 * @since 2019年6月21日
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static String format(final Date date, final String pattern) {
		return new SimpleDateFormat(pattern).format(date);
	}
	
	/**
	 * 按格式，获取当前时间
	 * @author liu.jie
	 * @since 2019年6月21日
	 * @param field
	 * @return
	 */
	public static Date now(int field) {
		return truncate(new Date(), field);
	}
	public static Date now() {
		return new Date();
	}
	
	/**
	 * 格式化日期
	 * @author liu.jie
	 * @since 2019年6月21日
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static String formatDate(final Date date, final String pattern) {
		return new SimpleDateFormat(pattern).format(date);
	}
	
	/**
	 * 标准格式化（yyyyMMddHHmmss）
	 * @param value
	 * @return
	 */
	public static String benchmark(Date value) {
		return formatDate(value, innerPatterns[7]);
	}
	/**
	 * 标准格式化（yyyyMMdd）
	 * @param value
	 * @return
	 */
	public static String benchmarkOfDate(Date value) {
		return formatDate(value, "yyyyMMdd");
	}
}
