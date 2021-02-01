package com.lc.plugin.sink.mongo;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;
import com.lc.plugin.sink.mongo.config.MdbConfig;
import com.lc.plugin.sink.mongo.service.MdbService;

/**
 * @author Lixiang
 * @description MongoDB发送器
 */
public class MongoSink extends SinkPluginAdapter{
	/**
	 * MongoDB配置
	 */
	private MdbConfig mdbConfig;
	
	/**
	 * MongoDB服务
	 */
	private MdbService mdbService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MongoSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("MongoSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.mdbConfig=new MdbConfig(flow).config();
		this.mdbService=new MdbService(mdbConfig);
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("MongoSink plugin handing...");
		if(flow.sinkStart) {
			log.info("MongoSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		if(!mdbService.preSend()) return false;
		
		try{
			if(mdbConfig.parse){
				if(null==mdbConfig.batchSize) {
					while(flow.sinkStart) {
						Boolean flag=mdbService.parseAndSingleSend(filterToSinkChannel.get());
						if(null!=flag && !flag) return false;
					}
				}else{
					while(flow.sinkStart) {
						Boolean flag=mdbService.parseAndBatchSend(filterToSinkChannel.get(mdbConfig.batchMaxWaitMills));
						if(null!=flag && !flag) return false;
					}
				}
			}else{
				if(null==mdbConfig.batchSize) {
					while(flow.sinkStart) {
						Boolean flag=mdbService.noParseAndSingleSend(filterToSinkChannel.get());
						if(null!=flag && !flag) return false;
					}
				}else{
					while(flow.sinkStart) {
						Boolean flag=mdbService.noParseAndBatchSend(filterToSinkChannel.get(mdbConfig.batchMaxWaitMills));
						if(null!=flag && !flag) return false;
					}
				}
			}
		}catch(InterruptedException e){
			log.warn("sink plugin is interrupted while waiting...");
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sinkStart=false;
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("MongoSink plugin config...");
		if(null==params || 0==params.length) return mdbConfig.collectRealtimeParams();
		if(params.length<2) return mdbConfig.getFieldValue((String)params[0]);
		return mdbConfig.setFieldValue((String)params[0],params[1]);
	}
}
