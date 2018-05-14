package com.ibeifeng.sparkproject.util;

/**
 * 字符串工具类
 * @author Administrator
 *
 */
public class StringUtils {

	/**
	 * 判断字符串是否为空
	 * @param str 字符串
	 * @return 是否为空
	 */
	public static boolean isEmpty(String str) {
		return str == null || "".equals(str);
	}
	
	/**
	 * 判断字符串是否不为空
	 * @param str 字符串
	 * @return 是否不为空
	 */
	public static boolean isNotEmpty(String str) {
		return str != null && !"".equals(str);
	}
	
	/**
	 * 截断字符串两侧的逗号
	 * @param str 字符串
	 * @return 字符串
	 */
	public static String trimComma(String str) {
		if(str.startsWith(",")) {
			str = str.substring(1);
		}
		if(str.endsWith(",")) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
	}
	
	/**
	 * 补全两位数字
	 * @param str
	 * @return
	 */
	public static String fulfuill(String str) {
		if(str.length() == 2) {
			return str;
		} else {
			return "0" + str;
		}
	}
	
	/**
	 * 从拼接的字符串中提取字段
	 * @param str 字符串
	 * @param delimiter 分隔符 
	 * @param field 字段
	 * @return 字段值
	 */
	//String str, String delimiter, String field(获取字段来自的内容，分隔符，要获取字段)
	public static String getFieldFromConcatString(String str, 
			String delimiter, String field) {
		try {
			String[] fields = str.split(delimiter);
			for(String concatField : fields) {
				// 这里加上if(concatField.split("=").length == 2)，是为了避免传入字符串为空，因为格式都是searchKeyWords="火锅"这种形式，所示用=号切割后，String数组的长度应该都为2
				//遍历字段
				if(concatField.split("=").length == 2) {
					//字段名
					String fieldName = concatField.split("=")[0];
					//字段值
					String fieldValue = concatField.split("=")[1];
					//如果分割出来的字段名和要获取的字段名相同
					if(fieldName.equals(field)) {
						return fieldValue;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 从拼接的字符串中给字段设置值
	 * @param str 字符串（原字符串）
	 * @param delimiter 分隔符 
	 * @param field 字段名
	 * @param newFieldValue 新的field值
	 * @return 字段值
	 */
	public static String setFieldInConcatString(String str, 
			String delimiter, String field, String newFieldValue) {
		String[] fields = str.split(delimiter);
		//遍历字符串的每个字段（key=value）
		for(int i = 0; i < fields.length; i++) {
			//取出每个字段的字段名
			String fieldName = fields[i].split("=")[0];
			//如果取出字段名和传入函数的字段名相同
			if(fieldName.equals(field)) {
				//给新的字段变成："fieldName = newFieldValue"的形式，就是把新的fieldValue赋值给fieldName
				String concatField = fieldName + "=" + newFieldValue;
				//更新String[] fields数组
				fields[i] = concatField;
				//跳出for循环（不仅是跳出if循环）
				break;
			}
		}
		
		//定义一个空字符串缓冲区buffer（字符串数组的长度不可变，java数组的长度均不可变）
		StringBuffer buffer = new StringBuffer("");
		for(int i = 0; i < fields.length; i++) {
			//把上面赋值newValue的字符串更新进字符串buffer
			buffer.append(fields[i]);
			//用"|"间隔开每个字段，最后一个字段除外
			if(i < fields.length - 1) {
				buffer.append("|");  
			}
		}
		//将buffer转成字符串数组
		return buffer.toString();
	}
	
}
