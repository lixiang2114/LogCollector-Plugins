package com.lc.plugin.transfer.http;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.TransferPluginAdapter;
import com.github.lixiang2114.netty.HttpServer;
import com.github.lixiang2114.netty.context.ServerConfig;
import com.lc.plugin.transfer.http.config.HttpConfig;
import com.lc.plugin.transfer.http.service.TransferService;

/**
 * @author Lixiang
 * @description HTTP转存器
 */
public class HttpTransfer extends TransferPluginAdapter {
	/**
	 * 嵌入式Web服务器
	 */
	private HttpServer httpServer;
	
	/**
	 * HTTP协议参数配置
	 */
	private HttpConfig httpConfig;
	
	/**
	 * HTTP服务器配置
	 */
	private ServerConfig serverConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HttpTransfer.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("HttpTransfer plugin starting...");
		File confFile=new File(pluginPath,"transfer.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		httpConfig=new HttpConfig(flow).config();
		serverConfig=new ServerConfig(httpConfig.port,httpConfig,TransferService.class);
		return true;
	}
	
	@Override
	public Object transfer(Channel<String> transferToSourceChannel) throws Exception {
		((HttpConfig)serverConfig.servletConfig).etlChannel=transferToSourceChannel;
		httpServer = new HttpServer(serverConfig);
		log.info("start transfer save process...");
		flow.transferStart = true;
		httpServer.startServer();
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.transferStart=false;
		httpServer.shutdownServer();
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("HttpTransfer plugin config...");
		if(null==params || 0==params.length) return httpConfig.collectRealtimeParams();
		if(params.length<2) return httpConfig.getFieldValue((String)params[0]);
		return httpConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.warn("HttpTransfer plugin not support reflesh checkpoint...");
		return true;
	}
}
