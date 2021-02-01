package com.lc.plugin.sink.mqtt.service;

import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lc.plugin.sink.mqtt.config.MqttConfig;

/**
 * @author Lixiang
 * @description Mqtt服务
 */
public class MqttService {
	/**
	 * Mqtt客户端
	 */
	public MqttClient mqttClient;
	
	/**
	 * Mqtt配置
	 */
	private MqttConfig mqttConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MqttService.class);
	
	public MqttService(){}
	
	public MqttService(MqttConfig mqttConfig){
		this.mqttConfig=mqttConfig;
		this.mqttClient=mqttConfig.mqttClient;
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws InterruptedException
	 */
	public boolean preSend() throws InterruptedException {
		if(0==mqttConfig.preFailSinkSet.size())  return true;
		List<MqttMessage> sinkedList=mqttConfig.preFailSinkSet.stream().map(e->{return (MqttMessage)e;}).collect(Collectors.toList());
		
		boolean loop=false;
		int times=0;
		do{
			try{
				mqttClient.publish(mqttConfig.getTopic(), sinkedList.get(0));
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(mqttConfig.failMaxWaitMills);
				log.error("publish occur excepton: "+e.getMessage());
			}
		}while(loop && times<mqttConfig.maxRetryTimes);
		
		if(!loop) mqttConfig.preFailSinkSet.clear();
		return !loop;
	}
	
	/**
	 * 发送Mqtt消息
	 * @param msg 消息内容
	 * @return 是否发送成功
	 * @throws InterruptedException
	 */
	public Boolean sendMsg(String msg) throws InterruptedException{
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
				Thread.sleep(mqttConfig.failMaxWaitMills);
				log.error("publish occur excepton: "+e.getMessage());
			}
		}while(loop && times<mqttConfig.maxRetryTimes);
		
		if(loop) mqttConfig.preFailSinkSet.add(message);
		return !loop;
	}
}
