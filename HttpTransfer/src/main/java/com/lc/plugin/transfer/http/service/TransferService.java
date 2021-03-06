package com.lc.plugin.transfer.http.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.netty.handlers.PrintWriter;
import com.github.lixiang2114.netty.scope.HttpServletRequest;
import com.github.lixiang2114.netty.scope.HttpServletResponse;
import com.github.lixiang2114.netty.servlet.HttpAction;
import com.lc.plugin.transfer.http.config.HttpConfig;
import com.lc.plugin.transfer.http.config.RecvType;

/**
 * @author Lixiang
 * @description 转存服务模块
 */
public class TransferService extends HttpAction{
	/**
	 * Http转存配置
	 */
	private HttpConfig httpConfig;
	
	/**
	 * 文件写出器
	 */
	private BufferedWriter fileWriter;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(TransferService.class);
	
	public TransferService() {}
	
	public TransferService(Object servletConfig) throws IOException{
		
	}
	
	@Override
	public void init() throws IOException {
		this.httpConfig=(HttpConfig)serverConfig.servletConfig;
		this.fileWriter=new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(httpConfig.transferSaveFile.toPath(), StandardOpenOption.APPEND)));
	}

	/**
	 * 为请求的Http客户端响应ACK确认信息
	 * @param response 响应对象
	 * @throws Exception
	 */
	private static final void writeACK(HttpServletResponse response,String ackMsg) throws Exception {
		PrintWriter writer=response.getPrintWriter();
		try{
			writer.write(ackMsg);
		}finally{
			writer.close();
		}
	}
	
	/**
	 * 客户端登录逻辑
	 * @param request 请求对象
	 * @param response 响应对象
	 * @throws Exception
	 */
	public boolean loginCheck(HttpServletRequest request, HttpServletResponse response) throws Exception {
		if(!httpConfig.requireLogin) return true;
		if(null!=request.getSession().getAttribute("loginUser")) return true;
		
		log.info("current client is not logged, verify the login...");
		
		String validateResult=httpConfig.loginFailureId;
		if(null==httpConfig.userField || null==httpConfig.passField || null==httpConfig.userName || null==httpConfig.passWord){
			log.info("login field is Null,Client Login Failure!");
		}else{
			String userField=httpConfig.userField.trim();
			String passField=httpConfig.passField.trim();
			String userNameField=httpConfig.userName.trim();
			String passWordField=httpConfig.passWord.trim();
			
			if(userField.isEmpty() || passField.isEmpty() || userNameField.isEmpty() || passWordField.isEmpty()){
				log.info("login field is Empty,Client Login Failure!");
			}else{
				String userName=request.getParameter(httpConfig.userField);
				String passWord=request.getParameter(httpConfig.passField);
				if(!httpConfig.userName.equals(userName) || !httpConfig.passWord.equals(passWord)) {
					log.info("userName or passWord is Error,Client Login Failure!");
				}else{
					log.info("Client Login Success!");
					validateResult=httpConfig.loginSuccessId;
					request.getSession().setAttribute("loginUser", Collections.singletonMap(userName, passWord));
				}
			}
		}
		
		writeACK(response,validateResult);
		return false;
	}
	
	@Override
	public void execute(HttpServletRequest request, HttpServletResponse response) throws Exception {
		//如果登录检查不通过则放弃数据
		if(!loginCheck(request,response)) return;
		
		//响应确认数据
		String returnData=httpConfig.normalReply;
		try {
			//获取服务接收到的数据
			String line=null;
			RecvType recvType=httpConfig.recvType;
			switch(recvType){
				case MessageBody:
					 line=request.getJsonBody();
					 break;
				case QueryString:
					 line=request.getQueryString();
					 break;
				case ParamMap:
					 line=CommonUtil.javaToJsonStr(request.getParametersMap());
					 break;
				default:
					log.info("Error: unknow recvType!");
					return;
			}
			
			if(null==line) return;
			
			//写入转存日志文件
			fileWriter.write(line);
			fileWriter.newLine();
			fileWriter.flush();
			
			//当前转存日志文件未达到最大值则继续写转存日志文件
			if(httpConfig.transferSaveFile.length()<httpConfig.transferSaveMaxSize) return;
			
			//当前转存日志文件达到最大值则增加转存日志文件
			String curTransSaveFilePath=httpConfig.transferSaveFile.getAbsolutePath();
			int lastIndex=curTransSaveFilePath.lastIndexOf(".");
			httpConfig.transferSaveFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
			fileWriter.close();
			
			log.info("HttpTransfer switch transfer save log file to: "+httpConfig.transferSaveFile.getAbsolutePath());
			fileWriter=new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(httpConfig.transferSaveFile.toPath(), StandardOpenOption.APPEND)));
		} catch (Exception e) {
			returnData=httpConfig.errorReply;
			log.error("transfer save process running error...",e);
		}finally{
			writeACK(response,returnData);
		}
	}
}
