package com.lc.plugin.source.ma.file.service;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Channel;
import com.lc.plugin.source.ma.file.config.FileConfig;

/**
 * @author Lixiang
 * @description 文件日志服务
 */
public class FileService {
	/**
	 * FileSource配置
	 */
	private FileConfig fileConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileService.class);
	
	public FileService(){}
	
	public FileService(FileConfig fileConfig){
		this.fileConfig=fileConfig;
	}
	
	/**
	 * 停止离线ETL流程
	 */
	public Boolean stopManualETLProcess(Object params) {
		log.info("stop manual ETL process...");
		fileConfig.flow.sourceStart=false;
		return true;
	}

	/**
	 * 启动离线ETL流程(读取预置日志文件)
	 */
	public Object startManualETLProcess(Channel<String> sourceToFilterChannel) {
		RandomAccessFile raf=null;
		log.info("execute manual ETL process...");
		File manualLogFile=fileConfig.manualLoggerFile;
		try{
			raf=new RandomAccessFile(manualLogFile, "r");
			if(0!=fileConfig.manualByteNumber)raf.seek(fileConfig.manualByteNumber);
			while(fileConfig.flow.sourceStart){
				fileConfig.manualLineNumber=fileConfig.manualLineNumber++;
				fileConfig.manualByteNumber=raf.getFilePointer();
				String line=raf.readLine();
				if(null==line) break;
				
				String record=line.trim();
				if(0==record.length()) continue;
				sourceToFilterChannel.put(record);
			}
			
			log.info("FileManual plugin manual etl process normal exit,execute checkpoint...");
			fileConfig.refreshCheckPoint();
		}catch(Exception e){
			e.printStackTrace();
			try {
				fileConfig.refreshCheckPoint();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			log.info("FileManual plugin manual etl process occur Error...",e);
		}finally{
			try{
				if(null!=raf) raf.close();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		return true;
	}
}
