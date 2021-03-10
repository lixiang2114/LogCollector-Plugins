package com.lc.plugin.sink.mongo.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.bson.Document;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.connection.ServerAddressHelper;

/**
 * @author Lixiang
 * @description MDB客户端配置
 */
@SuppressWarnings("unchecked")
public class MdbConfig {
	/**
	 * 处理记录中的ID字段
	 */
	public  String idField;
	
	/**
	 * 处理记录中的库名字段
	 */
	public String dbField;
	
	/**
	 * 处理记录中的表名字段
	 */
	public String tabField;
	
	/**
	 * 发送器运行时路径
	 */
	public File sinkPath;
	
	/**
	 * 是否解析通道数据记录
	 */
	public boolean parse;
	
	/**
	 * 登录MongoDB用户名
	 */
	public String userName;
	
	/**
	 * 登录MongoDB密码
	 */
	public String passWord;
	
	/**
	 * 默认数据库名
	 */
	public String defaultDB;
	
	/**
	 * 默认数据表名
	 */
	public String defaultTab;
	
	/**
	 * 记录字段列表
	 * 按记录行从左到右区分顺序
	 */
	public String[] fieldList;
	
	/**
	 * 批处理尺寸
	 */
	public Integer batchSize;
	
	/**
	 * MDB客户端配置
	 */
	public Properties config;
	
	/**
	 * GMT时差毫秒数
	 */
	public long timeZoneMillis;
	
	/**
	 * 记录字段默认分隔符为中英文空白正则式
	 */
	public Pattern fieldSeparator;
	
	/**
	 * 发送失败后最大等待时间间隔
	 */
	public Long failMaxWaitMills;
	
	/**
	 * 发送失败后最大重试次数
	 */
	public Integer maxRetryTimes;
	
	/**
	 * 批处理最大等待时间间隔
	 */
	public Long batchMaxWaitMills;
	
	/**
	 * MongoDB客户端
	 */
	public MongoClient mongoClient;
	
	/**
	 * 写入MDB数据库的时间字段集
	 * 通常为java.util.Date的子类型
	 */
	public Set<String> timeFieldSet;
	
	/**
	 * 写入MDB数据库的数字字段集
	 * 通常为java.lang.Number的子类型
	 */
	public Set<String> numFieldSet;
	
	/**
	 * 上次发送失败任务表
	 */
	public Set<Object> preFailSinkSet;
	
	/**
	 * 默认数据库(未配置默认为test)
	 */
	public MongoDatabase defaultDatabase;
	
	/**
	 * MongoDB客户端连接参数
	 */
	public MongoClientOptions mongoClientOptions;
	
	/**
	 * 默认集合表(未配置默认为test)
	 */
	public MongoCollection<Document> defaultCollection;
	
	/**
	 * 英文冒号正则式
	 */
	private static final Pattern COLON_REGEX=Pattern.compile(":");
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 时区正则式
	 */
	private static final Pattern TIMEZONE_REGEX=Pattern.compile("\\+|-");
	
	/**
     * 数字正则式
     */
	private static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
	 * 主机地址表
	 */
	public ArrayList<ServerAddress> hostList=new ArrayList<ServerAddress>();
	
	/**
     * IP地址正则式
     */
	private static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	public MdbConfig(){}
	
	public MdbConfig(Flow flow) {
		this.sinkPath=flow.sinkPath;
		this.preFailSinkSet=flow.preFailSinkSet;
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * @param config
	 */
	public MdbConfig config() {
		String defaultDBStr=config.getProperty("defaultDB");
		defaultDB=isEmpty(defaultDBStr)?null:defaultDBStr.trim();
		
		String defaultTabStr=config.getProperty("defaultTab");
		defaultTab=isEmpty(defaultTabStr)?null:defaultTabStr.trim();
		
		String dbFieldStr=config.getProperty("dbField");
		dbField=isEmpty(dbFieldStr)?null:dbFieldStr.trim();
		
		String tabFieldStr=config.getProperty("tabField");
		tabField=isEmpty(tabFieldStr)?null:tabFieldStr.trim();
		
		String timeZoneStr=config.getProperty("timeZone");
		if(isEmpty(timeZoneStr)) {
			timeZoneMillis=8*3600*1000;
		}else{
			timeZoneStr=timeZoneStr.trim();
			String[] timeZoneArr=TIMEZONE_REGEX.split(timeZoneStr);
			if(2>timeZoneArr.length) throw new RuntimeException("parameter value error: "+timeZoneStr+", example: GMT+8");
			timeZoneMillis=Integer.parseInt(timeZoneArr[1])*3600*1000;
			if(-1==timeZoneStr.indexOf("+")) timeZoneMillis=-timeZoneMillis;
		}
		
		String numFieldStr=config.getProperty("numFields");
		if(isEmpty(numFieldStr)) {
			numFieldSet=new HashSet<String>();
		}else{
			String[] numFieldArr=COMMA_REGEX.split(numFieldStr.trim());
			numFieldSet=Arrays.stream(numFieldArr).map(e->e.trim()).collect(Collectors.toSet());
		}
		
		String timeFieldStr=config.getProperty("timeFields");
		if(isEmpty(timeFieldStr)) {
			timeFieldSet=new HashSet<String>();
		}else{
			String[] timeFieldArr=COMMA_REGEX.split(timeFieldStr.trim());
			timeFieldSet=Arrays.stream(timeFieldArr).map(e->e.trim()).collect(Collectors.toSet());
		}
		
		initHostAddress();
		initMongoClientOptions();
		parse=Boolean.parseBoolean(getParamValue("parse", "true"));
		fieldSeparator=Pattern.compile(getParamValue("fieldSeparator","\\s+"));
		maxRetryTimes=Integer.parseInt(getParamValue("maxRetryTimes", "3"));
		failMaxWaitMills=Long.parseLong(getParamValue("failMaxTimeMills", "2000"));
		batchMaxWaitMills=Long.parseLong(getParamValue("batchMaxTimeMills", "2000"));
		
		String batchSizeStr=config.getProperty("batchSize");
		if(null!=batchSizeStr) {
			String tmp=batchSizeStr.trim();
			if(0!=tmp.length()) batchSize=Integer.parseInt(tmp);
		}
		
		String idFieldStr=config.getProperty("idField");
		if(null!=idFieldStr) {
			String tmp=idFieldStr.trim();
			if(0!=tmp.length()) idField=tmp;
		}
		
		String fieldListStr=config.getProperty("fieldList");
		if(null!=fieldListStr){
			String[] fields=COMMA_REGEX.split(fieldListStr);
			fieldList=new String[fields.length];
			for(int i=0;i<fields.length;i++){
				String fieldName=fields[i].trim();
				if(0==fieldName.length()){
					fieldList[i]="field"+i;
					continue;
				}
				fieldList[i]=fieldName;
			}
		}
		
		String passWordStr=config.getProperty("passWord");
		String userNameStr=config.getProperty("userName");
		if(null!=passWordStr && null!=userNameStr) {
			String pass=passWordStr.trim();
			String user=userNameStr.trim();
			if(0!=pass.length() && 0!=user.length()) {
				userName=user;
				passWord=pass;
			}
		}
		
		if(null==userName || null==passWord) {
			mongoClient=new MongoClient(hostList,mongoClientOptions);
		}else{
			String loginDB=null==defaultDB?"admin":defaultDB;
			mongoClient=new MongoClient(hostList,MongoCredential.createCredential(userName, loginDB, passWord.toCharArray()),mongoClientOptions);
		}
		
		defaultDatabase=mongoClient.getDatabase(null==defaultDB?"test":defaultDB);
		defaultCollection=defaultDatabase.getCollection(null==defaultTab?"test":defaultTab,Document.class);
		
		return this;
	}
	
	/**
	 * 初始化主机地址列表
	 */
	private void initHostAddress(){
		String[] hosts=COMMA_REGEX.split(getParamValue("hostList", "127.0.0.1:27017"));
		for(int i=0;i<hosts.length;i++){
			String host=hosts[i].trim();
			if(0==host.length()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2){
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				hostList.add(ServerAddressHelper.createServerAddress(ip, Integer.parseInt(port)));
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				hostList.add(ServerAddressHelper.createServerAddress("127.0.0.1", Integer.parseInt(unknow)));
			}else if(IP_REGEX.matcher(unknow).matches()){
				hostList.add(ServerAddressHelper.createServerAddress(unknow, 27017));
			}
		}
	}
	
	/**
	 * 初始化MongoDB连接参数
	 * @param context 插件配置上下文
	 */
	private void initMongoClientOptions() {
		Builder options = MongoClientOptions.builder();
		options.socketTimeout(Integer.parseInt(getParamValue("socketTimeout", "0")));
		options.maxWaitTime(Integer.parseInt(getParamValue("maxWaitTime", "5000")));
		options.connectTimeout(Integer.parseInt(getParamValue("connectTimeout", "30000")));
		options.connectionsPerHost(Integer.parseInt(getParamValue("connectionsPerHost", "300")));
		options.cursorFinalizerEnabled(Boolean.parseBoolean(getParamValue("cursorFinalizerEnabled", "true")));
		
		WriteConcern writeConcern=WriteConcern.UNACKNOWLEDGED;
		
		try{
			String w=config.getProperty("w");
			w=null==w?null:w.trim();
			w=null==w||0==w.length()?null:w;
			if(null!=w) writeConcern=writeConcern.withW(w);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		try{
			String wTimeoutMS=config.getProperty("wTimeoutMS");
			wTimeoutMS=null==wTimeoutMS?null:wTimeoutMS.trim();
			wTimeoutMS=null==wTimeoutMS||0==wTimeoutMS.length()?null:wTimeoutMS;
			if(null!=wTimeoutMS)writeConcern=writeConcern.withWTimeout(Long.parseLong(wTimeoutMS), TimeUnit.MILLISECONDS);
		}catch(Exception e){
			e.printStackTrace();
		}

		try{
			String journal=config.getProperty("journal");
			journal=null==journal?null:journal.trim();
			journal=null==journal||0==journal.length()?null:journal;
			if(null!=journal)writeConcern=writeConcern.withJournal(Boolean.parseBoolean(journal));
		}catch(Exception e){
			e.printStackTrace();
		}
		
		options.writeConcern(writeConcern);
		mongoClientOptions=options.build();
	}
	
	/**
	 * 获取参数值
	 * @param key 参数名
	 * @param defaultValue 默认参数值
	 * @return 参数值
	 */
	private String getParamValue(String key,String defaultValue){
		String value=config.getProperty(key, defaultValue).trim();
		return value.length()==0?defaultValue:value;
	}
	
	/**
	 * 获取参数值
	 * @param key 参数名
	 * @param defaultValue 默认参数值
	 * @return 参数值
	 */
	private static final boolean isEmpty(String value) {
		if(null==value) return true;
		return 0==value.trim().length();
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
			Field field=MdbConfig.class.getDeclaredField(attrName);
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
			Field field=MdbConfig.class.getDeclaredField(attrName);
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
		map.put("parse", parse);
		map.put("idField", idField);
		map.put("dbField", dbField);
		map.put("fieldList", fieldList);
		map.put("hostList", hostList);
		map.put("tabField", tabField);
		map.put("sinkPath", sinkPath);
		map.put("batchSize", batchSize);
		map.put("defaultDB", defaultDB);
		map.put("passWord", passWord);
		map.put("userName", userName);
		map.put("defaultTab", defaultTab);
		map.put("fieldSeparator", fieldSeparator);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("failMaxWaitMills", failMaxWaitMills);
		map.put("defaultDatabase", defaultDatabase);
		map.put("defaultCollection", defaultCollection);
		map.put("batchMaxWaitMills", batchMaxWaitMills);
		map.put("preFailSinkSetSize", preFailSinkSet.size());
		map.put("maxWaitTime", mongoClientOptions.getMaxWaitTime());
		map.put("socketTimeout", mongoClientOptions.getSocketTimeout());
		map.put("connectTimeout", mongoClientOptions.getConnectTimeout());
		map.put("connectionsPerHost", mongoClientOptions.getConnectionsPerHost());
		map.put("cursorFinalizerEnabled", mongoClientOptions.isCursorFinalizerEnabled());
		return map.toString();
	}
}
