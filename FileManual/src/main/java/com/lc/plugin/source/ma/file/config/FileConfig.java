package com.lc.plugin.source.ma.file.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description 文件配置
 */
@SuppressWarnings("unchecked")
public class FileConfig {
	/**
	 * 流程对象
	 */
	public Flow flow;
	
	/**
	 * 插件目录
	 */
	public File pluginPath;
	
	/**
	 * 离线推送的日志文件
	 */
	public File manualLoggerFile;
	
	/**
	 * 离线文件已经读取的行数
	 */
	public int manualLineNumber;
	
	/**
	 * 离线文件已经读取的字节数量
	 */
	public long manualByteNumber;
	
	/**
	 * 日志(检查点)配置
	 */
	public Properties loggerConfig;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileConfig.class);
	
	public FileConfig(){}
	
	public FileConfig(Flow flow){
		this.pluginPath=(this.flow=flow).sourcePath;
	}
	
	/**
	 * @param config
	 */
	public FileConfig config() {
		//读取日志文件配置
		File configFile=new File(pluginPath,"source.properties");
		loggerConfig=PropertiesReader.getProperties(configFile);
		
		//离线手动推送的日志文件
		String manualFileName=loggerConfig.getProperty("manualLoggerFile");
		if(null==manualFileName || 0==manualFileName.trim().length()) {
			log.warn("not found parameter: 'manualLoggerFile',can not offline send with manual...");
		}else{
			File file=new File(manualFileName.trim());
			if(file.exists() && file.isFile()) manualLoggerFile=file;
		}
		
		//离线手动推送的日志文件检查点
		manualLineNumber=Integer.parseInt(loggerConfig.getProperty("manualLineNumber","0"));
		log.info("manualLineNumber is: "+manualLineNumber);
		manualByteNumber=Integer.parseInt(loggerConfig.getProperty("manualByteNumber","0"));
		log.info("manualByteNumber is: "+manualByteNumber);
		
		return this;
	}
	
	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	public void refreshCheckPoint() throws IOException{
		loggerConfig.setProperty("manualLineNumber",""+manualLineNumber);
		loggerConfig.setProperty("manualByteNumber",""+manualByteNumber);
		loggerConfig.setProperty("manualLoggerFile", manualLoggerFile.getAbsolutePath());
		
		OutputStream fos=null;
		try{
			fos=new FileOutputStream(new File(pluginPath,"source.properties"));
			log.info("reflesh checkpoint...");
			loggerConfig.store(fos, "reflesh checkpoint");
		}finally{
			if(null!=fos) fos.close();
		}
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
			Field field=FileConfig.class.getDeclaredField(attrName);
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
			Field field=FileConfig.class.getDeclaredField(attrName);
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
		map.put("pluginPath", pluginPath);
		map.put("manualLoggerFile", manualLoggerFile);
		map.put("manualLineNumber", manualLineNumber);
		map.put("manualByteNumber", manualByteNumber);
		return map.toString();
	}
}
