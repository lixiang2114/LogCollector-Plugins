package com.lc.plugin.extend.filter.config;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;
import com.github.lixiang2114.script.dynamic.DyScript;

/**
 * @author Lixiang
 * 扩展过滤器配置
 */
@SuppressWarnings("unchecked")
public class FilterConfig {
	/**
	 * 过滤器运行时路径
	 */
	private File filterPath;
	
	/**
	 * 主脚本文件
	 */
	public File mainScript;
	
	/**
	 * 主脚本类名
	 */
	public String mainClass;
	
	/**
	 * 主脚本方法
	 */
	public String mainMethodDefine;
	
	/**
	 * 过滤器参数配置
	 */
	private Properties config;
	
	/**
	 * 入口函数名
	 */
	public static final String MAIN_METHOD="main";
	
	/**
	 * 英文点号正则式
	 */
	private static final Pattern DOT_REGEX=Pattern.compile("\\.");
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 空白正则式
	 */
	private static final Pattern BLANK_REGEX=Pattern.compile("\\s+");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FilterConfig.class);
	
	public FilterConfig(){}
	
	public FilterConfig(Flow flow){
		this.filterPath=flow.filterPath;
		this.config=PropertiesReader.getProperties(new File(filterPath,"filter.properties"));
	}
	
	/**
	 * 配置文件发送器
	 * @param config
	 */
	public FilterConfig config() {
		String mainScriptStr=config.getProperty("mainScript");
		if(null!=mainScriptStr) {
			mainScriptStr=mainScriptStr.trim();
			if(0!=mainScriptStr.length()) mainScript=new File(filterPath,"script/"+mainScriptStr);
		}
		
		if(null==mainScript) {
			mainScript=new File(filterPath,"script/DefaultClass.java");
		}else{
			if(mainScript.isDirectory()) {
				log.error("{} can not be directory...",mainScript);
				throw new RuntimeException(mainScript+" can not be directory...");
			}
		}
		
		mainClass=BLANK_REGEX.matcher(DOT_REGEX.split(mainScript.getName())[0].trim()).replaceAll("");
		
		try{
			mainMethodDefine=new String(Files.readAllBytes(mainScript.toPath()),Charset.defaultCharset()).trim();
			log.info("\nload mainClass: {}\nload mainMethod:\n{}\n",mainClass,mainMethodDefine);
			String[] methodParts=BLANK_REGEX.split(mainMethodDefine.substring(0, mainMethodDefine.indexOf("(")));
			if(!MAIN_METHOD.equals(methodParts[methodParts.length-1].trim())) throw new IOException("must define main method in main script file: 'public static Object main(Object ARG){...}'");
		}catch(IOException e) {
			log.error("read main script file occur error: ",e);
			throw new RuntimeException(e);
		}
		
		dynamicLinkLoad(mainMethodDefine);
		
		try{
			DyScript.addClass(mainClass);
			DyScript.addFunction(mainClass,mainMethodDefine);
		}catch(Exception e) {
			log.error("load main script occur error: ",e);
			throw new RuntimeException(e);
		}
		
		return this;
	}
	
	/**
	 * 动态递归解析、链接并装载类
	 * @param methodDefine 方法定义
	 */
	private static final void dynamicLinkLoad(String methodDefine) {
		//后期扩展此功能
	}
	
	/**
	 * 获取字段值
	 * @param key 键
	 * @return 返回字段值
	 */
	public Object getFieldValue(String key) {
		if(null==key) return null;
		String attrName=key.trim();
		if(attrName.isEmpty()) return null;
		
		try {
			Field field=FilterConfig.class.getDeclaredField(attrName);
			field.setAccessible(true);
			
			Object fieldVal=field.get(this);
			Class<?> fieldType=field.getType();
			if(!fieldType.isArray()) return fieldVal;
			
			int len=Array.getLength(fieldVal);
			StringBuilder builder=new StringBuilder("[");
			for(int i=0;i<len;builder.append(Array.get(fieldVal, i++)).append(","));
			if(builder.length()>1) builder.deleteCharAt(builder.length()-1);
			builder.append("]");
			return builder.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 设置字段值
	 * @param key 键
	 * @param value 值
	 * @return 返回设置后的值
	 */
	public Object setFieldValue(String key,Object value) {
		if(null==key || null==value) return null;
		String attrName=key.trim();
		if(attrName.isEmpty()) return null;
		
		try {
			Field field=FilterConfig.class.getDeclaredField(attrName);
			field.setAccessible(true);
			
			Class<?> fieldType=field.getType();
			Object fieldVal=null;
			if(String[].class==fieldType){
				fieldVal=COMMA_REGEX.split(value.toString());
			}else if(File.class==fieldType){
				fieldVal=new File(value.toString());
			}else if(CommonUtil.isSimpleType(fieldType)){
				fieldVal=CommonUtil.transferType(value, fieldType);
			}else{
				return null;
			}
			
			field.set(this, fieldVal);
			return value;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 收集实时参数
	 * @return
	 */
	public String collectRealtimeParams() {
		HashMap<String,Object> map=new HashMap<String,Object>();
		map.put("mainClass", mainClass);
		map.put("mainScript", mainScript);
		map.put("mainMethodDefine", mainMethodDefine);
		return map.toString();
	}
}
