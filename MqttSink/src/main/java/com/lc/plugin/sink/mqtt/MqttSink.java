package com.lc.plugin.sink.mqtt;

import java.io.File;
import java.nio.charset.Charset;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;
import com.lc.plugin.sink.mqtt.config.MqttConfig;
import com.lc.plugin.sink.mqtt.scheduler.TokenScheduler;

/**
 * @author Lixiang
 * @description MQTT发送器
 */
public class MqttSink extends SinkPluginAdapter{
	/**
	 * MQTT客户端
	 */
	private MqttClient mqttClient;
	
	/**
	 * MQTT客户端配置
	 */
	private MqttConfig mqttConfig;
	
	/**
	 * Token调度器
	 */
	private TokenScheduler tokenScheduler;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MqttSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("MqttSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.mqttConfig=new MqttConfig(flow);
		mqttConfig.config();
		
		mqttClient=mqttConfig.connectMqttServer();
		
		if(null==mqttConfig.startTokenScheduler || !mqttConfig.startTokenScheduler){
			log.info("token expire is -1,no need to start the scheduler!");
		}else{
			tokenScheduler=new TokenScheduler(mqttConfig);
			tokenScheduler.startTokenScheduler();
			log.info("expire token scheduler is already started...");
		}
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("MqttSink plugin handing...");
		if(flow.sinkStart) {
			log.info("MqttSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		try{
			String message=null;
			while(flow.sinkStart) {
				if(null==(message=filterToSinkChannel.get())) continue;
				sendMqttMsg(message);
			}
		}catch(InterruptedException e){
			log.warn("sink plugin is interrupted while waiting...");
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sinkStart=false;
		tokenScheduler.stopTokenScheduler();
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("MqttSink plugin config...");
		if(null==params || 0==params.length) return mqttConfig.collectRealtimeParams();
		if(params.length<2) return mqttConfig.getFieldValue((String)params[0]);
		return mqttConfig.setFieldValue((String)params[0],params[1]);
	}
	
	/**
	 * 发送Mqtt消息
	 * @param msg 消息内容
	 * @return 是否发送成功
	 * @throws InterruptedException
	 */
	private Boolean sendMqttMsg(String msg) throws InterruptedException{
		if(null==msg) return null;
		if(0==(msg=msg.trim()).length()) return null;
		
		MqttMessage message = new MqttMessage(msg.getBytes(Charset.forName("UTF-8")));
		message.setRetained(mqttConfig.getRetained());
		message.setQos(mqttConfig.getQos());
		
		boolean loop=false;
		int times=0;
		do{
			try{
				mqttClient.publish(mqttConfig.getTopic(), message);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(2000L);
				log.error("publish occur excepton: "+e.getMessage());
			}
		}while(loop && times<3);
		
		return !loop;
	}
}
