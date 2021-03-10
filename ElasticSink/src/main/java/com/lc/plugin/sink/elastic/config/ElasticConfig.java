package com.lc.plugin.sink.elastic.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description Elastic客户端配置
 */
@SuppressWarnings("unchecked")
public class ElasticConfig {
	/**
	 * 发送器运行时路径
	 */
	public File sinkPath;
	
	/**
	 * 文档ID字段名
	 */
	public  String idField;
	
	/**
	 * 是否解析通道数据记录
	 */
	public boolean parse;
	
	/**
	 * 处理记录中的索引库名
	 */
	public String indexField;
	
	/**
	 * 处理记录中的索引类型名
	 */
	public String typeField;
	
	/**
	 * 登录Elastic服务的用户名
	 */
	public String userName;
	
	/**
	 * 登录Elastic服务的密码
	 */
	public String passWord;
	
	/**
	 * 集群名称
	 */
	public String clusterName;
	
	/**
	 * 默认索引库名
	 */
	public String defaultIndex;
	
	/**
	 * 默认索引类型名
	 */
	public String defaultType;
	
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
	 * Elastic客户端配置
	 */
	public Properties config;
	
	/**
	 * 时区差值
	 */
	public String timeZone;
	
	/**
	 * 主机地址表
	 */
	private HttpHost[] hostList;
	
	/**
	 * ES集群客户端
	 */
	public RestClient restClient;
	
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
	 * 写入Elastic数据库的时间字段集
	 * 通常为java.util.Date的子类型
	 */
	public Set<String> timeFieldSet;
	
	/**
	 * 写入Elastic数据库的数字字段集
	 * 通常为java.lang.Number的子类型
	 */
	public Set<String> numFieldSet;
	
	/**
	 * 上次发送失败任务表
	 */
	public Set<Object> preFailSinkSet;
	
	/**
	 * 是否可用认证缓存(默认可用)
	 */
	public boolean enableAuthCache=true;
	
	/**
	 * 英文冒号正则式
	 */
	private static final Pattern COLON_REGEX=Pattern.compile(":");
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
     * 数字正则式
     */
	private static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
     * IP地址正则式
     */
	private static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	public ElasticConfig(){}
	
	public ElasticConfig(Flow flow) {
		this.sinkPath=flow.sinkPath;
		this.preFailSinkSet=flow.preFailSinkSet;
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * @param config
	 */
	public ElasticConfig config() {
		String defaultIndexStr=config.getProperty("defaultIndex");
		defaultIndex=isEmpty(defaultIndexStr)?"/test":("/"+defaultIndexStr.trim().toLowerCase());
		
		String defaultTypeStr=config.getProperty("defaultType");
		defaultType=isEmpty(defaultTypeStr)?"/_doc":("/"+defaultTypeStr.trim());
		
		String indexFieldStr=config.getProperty("indexField");
		indexField=isEmpty(indexFieldStr)?null:indexFieldStr.trim();
		
		String typeFieldStr=config.getProperty("typeField");
		typeField=isEmpty(typeFieldStr)?null:typeFieldStr.trim();
		
		String clusterNameStr=config.getProperty("clusterName");
		clusterName=isEmpty(clusterNameStr)?null:clusterNameStr.trim();
		
		String enableAuthCacheStr=config.getProperty("enableAuthCache");
		if(!isEmpty(clusterNameStr)) enableAuthCache=Boolean.parseBoolean(enableAuthCacheStr.trim());
		
		String timeZoneStr=config.getProperty("timeZone");
		timeZone=null==timeZoneStr?"":timeZoneStr.trim();
		
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
		
		RestClientBuilder builder=RestClient.builder(hostList);
		builder.setDefaultHeaders(new Header[]{new BasicHeader("Content-Type","application/json;charset=UTF-8")});
		
		if(null!=userName && null!=passWord) {
			CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(userName,passWord));
			
			builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
		        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
		        	if(!enableAuthCache)httpClientBuilder.disableAuthCaching();
		            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		        }
		    });
		}
		
		restClient=builder.build();
		
		return this;
	}
	
	/**
	 * 初始化主机地址列表
	 */
	private void initHostAddress(){
		String[] hosts=COMMA_REGEX.split(getParamValue("hostList", "127.0.0.1:9200"));
		hostList=new HttpHost[hosts.length];
		for(int i=0;i<hosts.length;i++){
			String host=hosts[i].trim();
			if(0==host.length()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2){
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				hostList[i]=new HttpHost(ip, Integer.parseInt(port), "http");
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				hostList[i]=new HttpHost("127.0.0.1", Integer.parseInt(unknow), "http");
			}else if(IP_REGEX.matcher(unknow).matches()){
				hostList[i]=new HttpHost(unknow, 9200, "http");
			}
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
			Field field=ElasticConfig.class.getDeclaredField(attrName);
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
			Field field=ElasticConfig.class.getDeclaredField(attrName);
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
		map.put("fieldList", fieldList);
		map.put("sinkPath", sinkPath);
		map.put("typeField", typeField);
		map.put("timeZone", timeZone);
		map.put("batchSize", batchSize);
		map.put("passWord", passWord);
		map.put("userName", userName);
		map.put("indexField", indexField);
		map.put("defaultType", defaultType);
		map.put("defaultIndex", defaultIndex);
		map.put("fieldSeparator", fieldSeparator);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("hostList", Arrays.toString(hostList));
		map.put("failMaxWaitMills", failMaxWaitMills);
		map.put("batchMaxWaitMills", batchMaxWaitMills);
		map.put("preFailSinkSetSize", preFailSinkSet.size());
		return map.toString();
	}
}
