package com.lc.plugin.sink.flow.config;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.Context;
import com.github.lixiang2114.flow.context.SizeUnit;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;
import com.lc.plugin.sink.flow.dto.FlowMapper;

/**
 * @author Lixiang
 * @description 流程发送器配置
 */
@SuppressWarnings("unchecked")
public class FlowConfig {
	/**
	 * 流程实例对象
	 */
	public Flow flow;
	
	/**
	 * 发送器运行时路径
	 */
	private File sinkPath;
	
	/**
	 * 消息转发模式
	 */
	public SendMode sendMode;
	
	/**
	 * 文件客户端配置
	 */
	private Properties loggerConfig;
	
	/**
	 * 转存目标文件最大尺寸
	 */
	public Long targetFileMaxSize;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FlowConfig.class);
	
	/**
	 * 实时转存流程表
	 */
	public ArrayList<FlowMapper> targetFileList=new ArrayList<FlowMapper>();
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	public FlowConfig(){}
	
	public FlowConfig(Flow flow){
		this.sinkPath=(this.flow=flow).sinkPath;
		this.loggerConfig=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * 配置文件发送器
	 * @param config
	 * @throws IOException 
	 */
	public FlowConfig config() throws Exception {
		//转发项列表
		String dispatcherItemStr=getParamValue("dispatcherItems","").trim();
		if(dispatcherItemStr.isEmpty()) {
			log.error("dispatcherItems is empty...");
			throw new RuntimeException("dispatcherItems is empty...");
		}
		
		//转存日志文件最大尺寸
		targetFileMaxSize=getTargetFileMaxSize();
		log.info("target file max size is: "+targetFileMaxSize);
		
		//转发模式
		String sendModeStr=getParamValue("sendMode","file").trim();
		sendMode=sendModeStr.isEmpty()?SendMode.valueOf("file"):SendMode.valueOf(sendModeStr);
		
		//转存文件列表
		HashSet<FlowMapper> transferSaveFiles=new HashSet<FlowMapper>();
		if(SendMode.file==sendMode) {
			String[] targetFiles=COMMA_REGEX.split(dispatcherItemStr);
			for(int i=0;i<targetFiles.length;i++) {
				File file=new File(targetFiles[i].trim());
				if(file.exists() && file.isDirectory()) {
					log.error("target file: {} can not be directory...",file.getAbsolutePath());
					throw new RuntimeException("target file: "+file.getAbsolutePath()+" can not be directory...");
				}
				transferSaveFiles.add(new FlowMapper(file,SendMode.file));
			}
		}else{
			String[] targetFlows=COMMA_REGEX.split(dispatcherItemStr);
			for(String targetFlowName:targetFlows) {
				String flowName=targetFlowName.trim();
				Flow targetFlow=Context.getFlow(flowName);
				if(null==targetFlow) {
					log.error("flow: {} is not exists",flowName);
					throw new RuntimeException("flow: "+flowName+" is not exists");
				}
				transferSaveFiles.add(new FlowMapper(targetFlow,sendMode));
			}
		}
		
		this.targetFileList.addAll(transferSaveFiles);
		log.info("target items are: "+transferSaveFiles);
		
		return this;
	}
	
	/**
	 * 获取转存日志文件最大尺寸(默认为2GB)
	 */
	private Long getTargetFileMaxSize(){
		String configMaxVal=getParamValue("targetFileMaxSize","");
		if(configMaxVal.isEmpty()) return 2*1024*1024*1024L;
		Matcher matcher=CAP_REGEX.matcher(configMaxVal);
		if(!matcher.find()) return 2*1024*1024*1024L;
		return SizeUnit.getBytes(Long.parseLong(matcher.group(1)), matcher.group(2).substring(0,1));
	}
	
	/**
	 * 获取参数值
	 * @param key 键
	 * @param defaultValue 默认值
	 */
	private String getParamValue(String key,String defaultValue) {
		String tmp=loggerConfig.getProperty(key, defaultValue).trim();
		return tmp.isEmpty()?defaultValue:tmp;
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
			Field field=FlowConfig.class.getDeclaredField(attrName);
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
			Field field=FlowConfig.class.getDeclaredField(attrName);
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
		map.put("sinkPath", sinkPath);
		map.put("sendMode", sendMode);
		map.put("targetFileList", targetFileList);
		map.put("targetFileMaxSize", targetFileMaxSize);
		return map.toString();
	}
}
