package com.lc.plugin.sink.mongo.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.util.CommonUtil;
import com.lc.plugin.sink.mongo.config.MdbConfig;

/**
 * @author Lixiang
 * @description MDB服务
 */
@SuppressWarnings("unchecked")
public class MdbService {
	/**
	 * MongoDB配置
	 */
	private MdbConfig mdbConfig;
	
	/**
	 * 批量文档表
	 */
	public ArrayList<Document> batchList=new ArrayList<Document>();
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MdbService.class);
	
	public MdbService(){}
	
	public MdbService(MdbConfig mdbConfig){
		this.mdbConfig=mdbConfig;
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws InterruptedException
	 */
	public boolean preSend() throws InterruptedException {
		if(0==mdbConfig.preFailSinkSet.size())  return true;
		List<Document> sinkedList=mdbConfig.preFailSinkSet.stream().map(e->{return (Document)e;}).collect(Collectors.toList());
		
		boolean loop=false;
		int times=0;
		do{
			try{
				mdbConfig.mongoCollection.insertMany(sinkedList);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(mdbConfig.failMaxWaitMills);
				log.error("send occur excepton: "+e.getMessage());
			}
		}while(loop && times<mdbConfig.maxRetryTimes);
		
		if(!loop) mdbConfig.preFailSinkSet.clear();
		return !loop;
	}
	
	/**
	 * 解析通道消息并发送到MDB
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean parseAndSingleSend(String msg) throws InterruptedException {
		if(null==msg) return null;
		if(0==(msg=msg.trim()).length()) return null;
		
		HashMap<String,Object> docMap=new HashMap<String,Object>();
		String[] fieldValues=mdbConfig.fieldSeparator.split(msg);
		if(null==mdbConfig.fieldList || 0==mdbConfig.fieldList.length){
			for(int i=0;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}else if(mdbConfig.fieldList.length>=fieldValues.length){
			for(int i=0;i<fieldValues.length;docMap.put(mdbConfig.fieldList[i], fieldValues[i]),i++);
		}else{
			int i=0;
			for(;i<mdbConfig.fieldList.length;docMap.put(mdbConfig.fieldList[i], fieldValues[i]),i++);
			for(;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}
		
		if(0==docMap.size()) return null;
		
		if(null!=mdbConfig.docId  && 0!=mdbConfig.docId.length()){
			String docIdVal=null;
			if(0!=(docIdVal=docMap.getOrDefault(mdbConfig.docId, "").toString().trim()).length()) docMap.put("_id", docIdVal);
		}
		
		return singleSend(new Document(docMap));
	}
	
	/**
	 * 解析通道消息并发送到MDB
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean parseAndBatchSend(String msg) throws InterruptedException {
		if(null==msg) {
			if(0==batchList.size()) return null;
			return batchSend(batchList);
		}
		
		if(0==(msg=msg.trim()).length()) return null;
		
		HashMap<String,Object> docMap=new HashMap<String,Object>();
		String[] fieldValues=mdbConfig.fieldSeparator.split(msg);
		if(null==mdbConfig.fieldList || 0==mdbConfig.fieldList.length){
			for(int i=0;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}else if(mdbConfig.fieldList.length>=fieldValues.length){
			for(int i=0;i<fieldValues.length;docMap.put(mdbConfig.fieldList[i], fieldValues[i]),i++);
		}else{
			int i=0;
			for(;i<mdbConfig.fieldList.length;docMap.put(mdbConfig.fieldList[i], fieldValues[i]),i++);
			for(;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}
		
		if(0==docMap.size()) return null;
		
		if(null!=mdbConfig.docId  && 0!=mdbConfig.docId.length()){
			String docIdVal=null;
			if(0!=(docIdVal=docMap.getOrDefault(mdbConfig.docId, "").toString().trim()).length()) docMap.put("_id", docIdVal);
		}
		
		batchList.add(new Document(docMap));
		if(batchList.size()<mdbConfig.batchSize) return null;
		
		return batchSend(batchList);
	}
	
	/**
	 * 不解析通道消息并实时发送到MDB
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean noParseAndSingleSend(String msg) throws InterruptedException {
		if(null==msg) return null;
		if(0==(msg=msg.trim()).length()) return null;
		HashMap<String,Object> docMap=CommonUtil.jsonStrToJava(msg, HashMap.class);
		if(null==docMap || 0==docMap.size()) return null;
		
		if(null!=mdbConfig.docId  && 0!=mdbConfig.docId.length()){
			String docIdVal=null;
			if(0!=(docIdVal=docMap.getOrDefault(mdbConfig.docId, "").toString().trim()).length()) docMap.put("_id", docIdVal);
		}
		
		return singleSend(new Document(docMap));
	}
	
	/**
	 * 不解析通道消息直接发送到MDB
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean noParseAndBatchSend(String msg) throws InterruptedException {
		if(null==msg) {
			if(0==batchList.size()) return null;
			return batchSend(batchList);
		}
		
		if(0==(msg=msg.trim()).length()) return null;
		HashMap<String,Object> docMap=CommonUtil.jsonStrToJava(msg, HashMap.class);
		if(null==docMap || 0==docMap.size()) return null;
		
		if(null!=mdbConfig.docId  && 0!=mdbConfig.docId.length()){
			String docIdVal=null;
			if(0!=(docIdVal=docMap.getOrDefault(mdbConfig.docId, "").toString().trim()).length()) docMap.put("_id", docIdVal);
		}
		
		batchList.add(new Document(docMap));
		if(batchList.size()<mdbConfig.batchSize) return null;
		
		return batchSend(batchList);
	}
	
	/**
	 * 发送单个文档到MDB
	 * @param doc 文档对象
	 * @return 是否发送成功
	 * @throws InterruptedException 
	 */
	private boolean singleSend(Document doc) throws InterruptedException {
		boolean loop=false;
		int times=0;
		do{
			try{
				mdbConfig.mongoCollection.insertOne(doc);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(mdbConfig.failMaxWaitMills);
				log.error("send occur excepton: ",e);
			}
		}while(loop && times<mdbConfig.maxRetryTimes);
		
		if(loop) mdbConfig.preFailSinkSet.add(doc);
		return !loop;
	}
	
	/**
	 * 批量发送文档到MDB
	 * @return 是否发送成功
	 * @throws InterruptedException 
	 */
	private boolean batchSend(List<Document> batchDocList) throws InterruptedException {
		boolean loop=false;
		int times=0;
		do{
			try{
				mdbConfig.mongoCollection.insertMany(batchDocList);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(mdbConfig.failMaxWaitMills);
				log.error("send occur excepton: ",e);
			}
		}while(loop && times<mdbConfig.maxRetryTimes);
		
		if(loop) mdbConfig.preFailSinkSet.addAll(batchDocList);
		batchDocList.clear();
		return !loop;
	}
}
