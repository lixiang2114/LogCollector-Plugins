package com.lc.plugin.sink.sql.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description SQL客户端配置
 */
@SuppressWarnings("unchecked")
public class SqlConfig {
	/**
	 * SQL服务器数据表
	 */
	public String table;
	
	/**
	 * 发送器运行时路径
	 */
	public File sinkPath;
	
	/**
	 * 是否解析通道数据记录
	 */
	public boolean parse;
	
	/**
	 * SQL服务器用户
	 */
	public String userName;
	
	/**
	 * SQL服务器密码
	 */
	public String passWord;
	
	/**
	 * 推送数据SQL语句
	 */
	public String insertSQL;
	
	/**
	 * 批处理尺寸
	 */
	public Integer batchSize;
	
	/**
	 * SQL客户端配置
	 */
	public Properties config;
	
	/**
	 * JDBC驱动连接器
	 */
	private String jdbcDriver;
	
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
	 * 记录字段默认分隔符为中英文空白正则式
	 */
	public Pattern fieldSeparator;
	
	/**
	 * SQL客户端连接
	 */
	private Connection connection;
	
	/**
	 * SQL服务器连接字符串
	 */
	public String connectionString;
	
	/**
	 * 上次发送失败任务表
	 */
	public Set<Object> preFailSinkSet;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(SqlConfig.class);
	
	/**
	 * 记录字段类型列表
	 */
	public LinkedHashMap<String,Class<?>> fieldMap=new LinkedHashMap<String,Class<?>>();
	
	public SqlConfig(){}
	
	public SqlConfig(Flow flow) {
		this.sinkPath=flow.sinkPath;
		this.preFailSinkSet=flow.preFailSinkSet;
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * @param config
	 */
	public SqlConfig config() {
		String jdbcDriverStr=config.getProperty("jdbcDriver");
		if(isEmpty(jdbcDriverStr)) {
			log.error("No Jdbc Driver Specified,Parameter Name: jdbcDriver");
			throw new RuntimeException("No Jdbc Driver Specified,Parameter Name: jdbcDriver");
		}else{
			this.jdbcDriver=jdbcDriverStr.trim();
		}
		
		String connectionString=config.getProperty("connectionString");
		if(isEmpty(connectionString)) {
			log.error("No Connection String Specified,Parameter Name: connectionString");
			throw new RuntimeException("No Connection String Specified,Parameter Name: connectionString");
		}else{
			this.connectionString=connectionString.trim();
		}
		
		String tableStr=config.getProperty("table");
		if(isEmpty(tableStr)) {
			log.error("No Table Name Specified,Parameter Name: table");
			throw new RuntimeException("No Table Name Specified,Parameter Name: table");
		}else{
			this.table=tableStr.trim();
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
		
		try {
			Class.forName(jdbcDriver);
		} catch (ClassNotFoundException e) {
			log.error("Can Not Load And Init Jdbc Driver:",e);
			throw new RuntimeException(e);
		}
		
		try{
			this.getConnection();
		}catch(SQLException e){
			log.error("create connection occur exeption:",e);
			throw new RuntimeException(e);
		}
		
		try {
			this.initSQLConfig();
		} catch (Exception e) {
			log.error("initing sql client config occur exeption:",e);
			throw new RuntimeException(e);
		}
		
		String batchSizeStr=config.getProperty("batchSize");
		if(null!=batchSizeStr) {
			String tmp=batchSizeStr.trim();
			if(0!=tmp.length()) batchSize=Integer.parseInt(tmp);
		}
		
		parse=Boolean.parseBoolean(getParamValue("parse", "true"));
		fieldSeparator=Pattern.compile(getParamValue("fieldSeparator","\\s+"));
		maxRetryTimes=Integer.parseInt(getParamValue("maxRetryTimes", "3"));
		failMaxWaitMills=Long.parseLong(getParamValue("failMaxTimeMills", "2000"));
		batchMaxWaitMills=Long.parseLong(getParamValue("batchMaxTimeMills", "2000"));
		
		return this;
	}
	
	/**
	 * 初始化SQL配置
	 */
	public Connection getConnection() throws SQLException {
		if(null!=connection && !connection.isClosed()) return connection;
		if(null==userName || null==passWord) {
			return connection=DriverManager.getConnection(connectionString);
		}else{
			return connection=DriverManager.getConnection(connectionString, userName, passWord);
		}
	}
	
	/**
	 * 初始化SQL配置
	 */
	private void initSQLConfig() throws Exception {
		ResultSet res=null;
		Statement stat=null;
		StringBuilder sqlBuilder=new StringBuilder("insert into ").append(table).append(" (");
		try {
			stat=connection.createStatement();
			res=stat.executeQuery("select * from "+table+" where 1=2");
			ResultSetMetaData rsmd=res.getMetaData();
			int len=rsmd.getColumnCount();
			if(1>len) {
				log.error("Not Found Any Field From table: {}",table);
				throw new RuntimeException("Not Found Any Field From table: "+table);
			}
			
			String fieldName=null;
			for(int i=1;i<=len;i++){
				fieldName=rsmd.getColumnLabel(i);
				sqlBuilder.append(fieldName).append(",");
				fieldMap.put(fieldName, Class.forName(rsmd.getColumnClassName(i)));
			}
			
			sqlBuilder.replace(sqlBuilder.length()-1, sqlBuilder.length(), ") values (");
			for(int i=1;i<=len;sqlBuilder.append("?,"),i++);
			sqlBuilder.replace(sqlBuilder.length()-1, sqlBuilder.length(), ")");
			insertSQL=sqlBuilder.toString();
			
			log.info("fieldMap: {}",fieldMap);
			log.info("insertSQL: {}",insertSQL);
		} finally {
			if(null!=res) res.close();
			if(null!=stat) stat.close();
		}
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
		String valueStr=value.trim();
		return 0==valueStr.length();
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
			Field field=SqlConfig.class.getDeclaredField(attrName);
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
			Field field=SqlConfig.class.getDeclaredField(attrName);
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
		map.put("table", table);
		map.put("parse", parse);
		map.put("sinkPath", sinkPath);
		map.put("fieldMap", fieldMap);
		map.put("batchSize", batchSize);
		map.put("insertSQL", insertSQL);
		map.put("passWord", passWord);
		map.put("userName", userName);
		map.put("jdbcDriver", jdbcDriver);
		map.put("fieldSeparator", fieldSeparator);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("failMaxWaitMills", failMaxWaitMills);
		map.put("connectionString", connectionString);
		map.put("batchMaxWaitMills", batchMaxWaitMills);
		map.put("preFailSinkSetSize", preFailSinkSet.size());
		return map.toString();
	}
}
