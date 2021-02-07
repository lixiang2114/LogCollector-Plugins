package com.lc.plugin.transfer.file.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Channel;
import com.lc.plugin.transfer.file.config.FileConfig;

/**
 * @author Lixiang
 * @description 文件日志服务
 */
public class FileService {
	/**
	 * JVM系统运行时
	 */
	private Runtime runtime;
	
	/**
	 * Tailf子进程
	 */
	private Process tailfSubProcess;
	
	/**
	 * Tailf启动命令
	 */
	private String tailfStartCommand;
	
	/**
	 * Tailf停止命令
	 */
	private String tailfStopCommand;
	
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
		runtime=Runtime.getRuntime();
		
		if(FileConfig.isWin){
			tailfStopCommand="taskkill /F /IM tailf.exe";
			tailfStartCommand=FileConfig.winTailfCmd+" "+fileConfig.appLogFile;
		}else{
			tailfStopCommand="pkill tail";
			tailfStartCommand="tail -F "+fileConfig.appLogFile;
		}
		
		log.info("tailf start command is: "+tailfStartCommand);
	}

	/**
	 * 启动转存日志服务
	 */
	public Object startLogTransferSave(Channel<String> transferToETLChannel) {
		BufferedWriter bw=null;
		LineNumberReader lnr=null;
		log.info("start transfer save process...");
		
		try {
			String line=null;
			tailfSubProcess=runtime.exec(tailfStartCommand);
			lnr=new LineNumberReader(new InputStreamReader(tailfSubProcess.getInputStream()));
			
			out:while(fileConfig.flow.transferStart){
				bw=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileConfig.transferSaveFile,true)));
				while(fileConfig.flow.transferStart){
					if(null==(line=lnr.readLine())){
						try{
							Thread.sleep(1000L);
						}catch(InterruptedException e){
							log.warn("transfer Save sleep interrupted,transfer is over...");
							break out;
						}
						
						continue;
					}
					
					//写入转存日志文件
					bw.write(line);
					bw.newLine();
					bw.flush();
					
					//当前转存日志文件未达到最大值则继续写转存日志文件
					if(fileConfig.transferSaveFile.length()<fileConfig.transferSaveMaxSize) continue;
					
					//当前转存日志文件达到最大值则增加转存日志文件
					String curTransSaveFilePath=fileConfig.transferSaveFile.getAbsolutePath();
					int lastIndex=curTransSaveFilePath.lastIndexOf(".");
					fileConfig.transferSaveFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
					log.info("FileTransfer switch transfer save log file to: "+fileConfig.transferSaveFile.getAbsolutePath());
					bw.close();
					break;
				}
			}
		} catch (Exception e) {
			log.error("transfer save process running error...",e);
		}finally{
			try{
				if(null!=bw) bw.close();
				if(null!=lnr) lnr.close();
			}catch(IOException e){
				log.error("transfer save process close error...",e);
			}
		}
		
		return true;
	}
	
	/**
	 * 停止转存日志服务
	 */
	public Boolean stopLogTransferSave(Object params) {
		log.info("stop transfer save process...");
		if(!tailfSubProcess.isAlive()) return true;
		
		fileConfig.flow.transferStart=false;
		if((tailfSubProcess.destroyForcibly()).isAlive()){
			try {
				runtime.exec(tailfStopCommand);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return tailfSubProcess.isAlive();
	}
}
