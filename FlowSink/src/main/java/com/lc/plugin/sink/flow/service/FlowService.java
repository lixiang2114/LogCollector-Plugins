package com.lc.plugin.sink.flow.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lc.plugin.sink.flow.config.FlowConfig;
import com.lc.plugin.sink.flow.dto.FlowMapper;

/**
 * @author Lixiang
 * @description 流程服务模块
 */
public class FlowService {
	/**
	 * 流程发送器配置
	 */
	private FlowConfig flowConfig;
	
	/**
	 * 转存流程列表
	 */
	private ArrayList<FlowMapper> targetFileList;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FlowService.class);
	
	public FlowService(){}
	
	public FlowService(FlowConfig flowConfig){
		this.flowConfig=flowConfig;
		this.targetFileList=flowConfig.targetFileList;
	}
	
	/**
	 * 写出通道数据
	 * @param message 消息参数
	 * @throws Exception 
	 */
	public boolean writeChannel(String message) {
		log.info("start flow sink process...");
		try {
			int len=targetFileList.size();
			for(int i=0;i<len;targetFileList.get(i++).writeChannel(message));
			return true;
		} catch (Exception e) {
			log.error("flow sink process running error...",e);
			return false;
		}
	}
	
	/**
	 * 写出文件数据
	 * @param message 消息参数
	 * @throws Exception 
	 */
	public boolean writeFileLine(String message) {
		BufferedWriter bw=null;
		log.info("start flow sink process...");
		
		try {
			File targetFile=null;
			for(FlowMapper flowMapper:targetFileList) {
				flowMapper.writeFileLine(message);
				
				//当前转存日志文件未达到最大值则继续写转存日志文件
				if(flowConfig.targetFileMaxSize>(targetFile=flowMapper.getTargetFile()).length()) continue;
				
				//当前转存日志文件达到最大值则增加转存日志文件
				String curTransSaveFilePath=targetFile.getAbsolutePath();
				int lastIndex=curTransSaveFilePath.lastIndexOf(".");
				if(-1==lastIndex) {
					lastIndex=curTransSaveFilePath.length();
					curTransSaveFilePath=curTransSaveFilePath+".0";
				}
				
				File newTargetFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
				flowMapper.setTargetFile(newTargetFile);
				
				log.info("FlowSink switch flow: {} target file to: {}",flowMapper.getFlowName(),newTargetFile.getAbsolutePath());
			}
			return true;
		} catch (Exception e) {
			log.error("flow sink process running error...",e);
			return false;
		}finally{
			try{
				if(null!=bw) bw.close();
			}catch(IOException e){
				log.error("flow sink process close error...",e);
			}
		}
	}
	
	/**
	 * 停止文件流进程
	 * @throws IOException
	 */
	public void stop() throws IOException {
		int len=targetFileList.size();
		for(int i=0;i<len;targetFileList.get(i++).closeFileStream());
	}
}
