package com.lc.plugin.sink.sql.service;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.util.CommonUtil;
import com.lc.plugin.sink.sql.config.SqlConfig;

/**
 * @author Lixiang
 * @description SQL服务模块
 */
@SuppressWarnings("unchecked")
public class SqlService {
	/**
	 * SQL客户端配置
	 */
	private SqlConfig sqlConfig;
	
	/**
	 * SQL预编译语句
	 */
	private PreparedStatement pstat;
	
	/**
	 * 批量记录表
	 */
	public ArrayList<Object[]> batchList=new ArrayList<Object[]>();
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(SqlService.class);
	
	public SqlService(){}
	
	public SqlService(SqlConfig sqlConfig){
		this.sqlConfig=sqlConfig;
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws Exception
	 */
	public boolean preSend() throws Exception {
		if(0==sqlConfig.preFailSinkSet.size())return true;
		Iterator<Map.Entry<String,Class<?>>> typeList=sqlConfig.fieldMap.entrySet().iterator();
		if(null==pstat || pstat.isClosed()) pstat=sqlConfig.getConnection().prepareStatement(sqlConfig.insertSQL);
		List<Object[]> sinkedList=sqlConfig.preFailSinkSet.stream().map(e->{return (Object[])e;}).collect(Collectors.toList());
		
		int fieldNum=sqlConfig.fieldMap.size();
		for(Object[] record:sinkedList) {
			if(fieldNum>record.length) continue;
			for(int i=0;typeList.hasNext();pstat.setObject(i+1, CommonUtil.transferType(record[i], typeList.next().getValue())),i++);
			pstat.addBatch();
		}
	
		boolean loop=false;
		int times=0;
		do{
			try{
				int[] counts=pstat.executeBatch();
				pstat.clearParameters();
				pstat.clearBatch();
				if(counts.length!=sinkedList.size()) throw new RuntimeException("executeBatch failure: counts.length!=sinkedList.size");
				for(int count:counts) if(count<=0) throw new RuntimeException("executeBatch failure: count<=0");
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(sqlConfig.failMaxWaitMills);
				log.error("call preSend occur excepton: "+e.getMessage());
			}
		}while(loop && times<sqlConfig.maxRetryTimes);
		
		if(!loop) sqlConfig.preFailSinkSet.clear();
		return !loop;
	}
	
	/**
	 * 解析通道消息并发送到SQL数据库
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws Exception
	 */
	public Boolean parseAndSingleSend(String msg) throws Exception {
		if(null==msg) return null;
		if(0==(msg=msg.trim()).length()) return null;
		
		String[] record=sqlConfig.fieldSeparator.split(msg);
		if(record.length<sqlConfig.fieldMap.size()) {
			log.error("call parseAndSingleSend occur error: record.length<sqlConfig.fieldMap.size");
			throw new RuntimeException("call parseAndSingleSend occur error: record.length<sqlConfig.fieldMap.size");
		}
		
		return singleSend(record);
	}
	
	/**
	 * 解析通道消息并发送到SQL数据库
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws Exception
	 */
	public Boolean parseAndBatchSend(String msg) throws Exception {
		if(null==msg) {
			if(0==batchList.size()) return null;
			return batchSend(batchList);
		}
		
		if(0==(msg=msg.trim()).length()) return null;
		
		String[] record=sqlConfig.fieldSeparator.split(msg);
		if(record.length<sqlConfig.fieldMap.size()) {
			log.error("call parseAndBatchSend occur error: record.length<sqlConfig.fieldMap.size");
			throw new RuntimeException("call parseAndBatchSend occur error: record.length<sqlConfig.fieldMap.size");
		}
		
		batchList.add(record);
		if(batchList.size()<sqlConfig.batchSize) return null;
		
		return batchSend(batchList);
	}
	
	/**
	 * 不解析通道消息并实时发送到SQL数据库
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws Exception
	 */
	public Boolean noParseAndSingleSend(String msg) throws Exception {
		if(null==msg) return null;
		if(0==(msg=msg.trim()).length()) return null;
		LinkedHashMap<String, Class<?>> fieldMap=sqlConfig.fieldMap;
		
		HashMap<String,Object> recordMap=CommonUtil.jsonStrToJava(msg, HashMap.class);
		if(null==recordMap || fieldMap.size()>recordMap.size()) {
			log.error("call noParseAndSingleSend occur error: json message parse failure or recordMap.length<sqlConfig.fieldMap.size");
			throw new RuntimeException("call noParseAndSingleSend occur error:  json message parse failure or recordMap.length<sqlConfig.fieldMap.size");
		}
		
		Object[] record=new Object[fieldMap.size()];
		Iterator<Entry<String, Class<?>>> its=fieldMap.entrySet().iterator();
		for(int i=0;its.hasNext();record[i++]=recordMap.get(its.next().getKey()));
		
		return singleSend(record);
	}
	
	/**
	 * 不解析通道消息直接发送到SQL数据库
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws Exception
	 */
	public Boolean noParseAndBatchSend(String msg) throws Exception {
		if(null==msg) {
			if(0==batchList.size()) return null;
			return batchSend(batchList);
		}
		
		if(0==(msg=msg.trim()).length()) return null;
		
		LinkedHashMap<String, Class<?>> fieldMap=sqlConfig.fieldMap;
		HashMap<String,Object> recordMap=CommonUtil.jsonStrToJava(msg, HashMap.class);
		if(null==recordMap || fieldMap.size()>recordMap.size()) {
			log.error("call noParseAndBatchSend occur error: json message parse failure or recordMap.length<sqlConfig.fieldMap.size");
			throw new RuntimeException("call noParseAndBatchSend occur error:  json message parse failure or recordMap.length<sqlConfig.fieldMap.size");
		}
		
		Object[] record=new Object[fieldMap.size()];
		Iterator<Entry<String, Class<?>>> its=fieldMap.entrySet().iterator();
		for(int i=0;its.hasNext();record[i++]=recordMap.get(its.next().getKey()));
		
		batchList.add(record);
		if(batchList.size()<sqlConfig.batchSize) return null;
		
		return batchSend(batchList);
	}
	
	/**
	 * 发送单条记录到SQL数据库
	 * @param record 记录对象
	 * @return 是否发送成功
	 * @throws Exception 
	 */
	private boolean singleSend(Object[] record) throws Exception {
		Iterator<Map.Entry<String,Class<?>>> typeList=sqlConfig.fieldMap.entrySet().iterator();
		if(null==pstat || pstat.isClosed()) pstat=sqlConfig.getConnection().prepareStatement(sqlConfig.insertSQL);
		
		try{
			for(int i=0;typeList.hasNext();pstat.setObject(i+1, CommonUtil.transferType(record[i], typeList.next().getValue())),i++);
			
			boolean loop=false;
			int times=0;
			do{
				try{
					if(0>=pstat.executeUpdate()) throw new RuntimeException("call singleSend failure: count<=0");
					pstat.clearParameters();
					loop=false;
				}catch(Exception e) {
					times++;
					loop=true;
					Thread.sleep(sqlConfig.failMaxWaitMills);
					log.error("call singleSend excepton: ",e);
				}
			}while(loop && times<sqlConfig.maxRetryTimes);
			
			if(loop) sqlConfig.preFailSinkSet.add(record);
			return !loop;
		}finally{
			if(null!=pstat) pstat.close();
		}
	}
	
	/**
	 * 批量发送记录到SQL数据库
	 * @param recordList 记录列表
	 * @return 是否发送成功
	 * @throws Exception 
	 */
	private boolean batchSend(List<Object[]> recordList) throws Exception {
		Iterator<Map.Entry<String,Class<?>>> typeList=sqlConfig.fieldMap.entrySet().iterator();
		if(null==pstat || pstat.isClosed()) pstat=sqlConfig.getConnection().prepareStatement(sqlConfig.insertSQL);
		
		for(Object[] record:recordList) {
			for(int i=0;typeList.hasNext();pstat.setObject(i+1, CommonUtil.transferType(record[i], typeList.next().getValue())),i++);
			pstat.addBatch();
		}
	
		boolean loop=false;
		int times=0;
		do{
			try{
				int[] counts=pstat.executeBatch();
				pstat.clearParameters();
				pstat.clearBatch();
				if(counts.length!=recordList.size()) throw new RuntimeException("executeBatch failure: counts.length!=recordList.size");
				for(int count:counts) if(count<=0) throw new RuntimeException("executeBatch failure: count<=0");
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(sqlConfig.failMaxWaitMills);
				log.error("call batchSend excepton: ",e);
			}
		}while(loop && times<sqlConfig.maxRetryTimes);
		
		if(loop) sqlConfig.preFailSinkSet.addAll(recordList);
		recordList.clear();
		return !loop;
	}
}
