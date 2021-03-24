package com.lc.plugin.sink.flow;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;
import com.lc.plugin.sink.flow.config.FlowConfig;
import com.lc.plugin.sink.flow.config.SendMode;
import com.lc.plugin.sink.flow.service.FlowService;

/**
 * @author Lixiang
 * @description 流程转发器
 * 本插件可用于实现分流逻辑
 */
public class FlowSink extends SinkPluginAdapter{
	/**
	 * 流程配置
	 */
	private FlowConfig flowConfig;
	
	/**
	 * 文件服务配置
	 */
	private FlowService flowService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FlowSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("FlowSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.flowConfig=new FlowConfig(flow).config();
		this.flowService=new FlowService(flowConfig);
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("FlowSink plugin handing...");
		if(flow.sinkStart) {
			log.info("FlowSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		
		try{
			String message=null;
			if(SendMode.channel==flowConfig.sendMode){
				while(flow.sinkStart) {
					if(null==(message=filterToSinkChannel.get())) continue;
					if((message=message.trim()).isEmpty()) continue;
					if(!flowService.writeChannel(message)) return false;
				}
			}else{
				while(flow.sinkStart) {
					if(null==(message=filterToSinkChannel.get())) continue;
					if((message=message.trim()).isEmpty()) continue;
					if(!flowService.writeFileLine(message)) return false;
				}
			}
		}catch(Exception e){
			log.warn("sink plugin is interrupted while waiting...");
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sinkStart=false;
		flowService.stop();
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("FlowSink plugin config...");
		if(null==params || 0==params.length) return flowConfig.collectRealtimeParams();
		if(params.length<2) return flowConfig.getFieldValue((String)params[0]);
		return flowConfig.setFieldValue((String)params[0],params[1]);
	}
}
