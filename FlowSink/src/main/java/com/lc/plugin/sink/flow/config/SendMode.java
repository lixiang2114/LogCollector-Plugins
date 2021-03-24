package com.lc.plugin.sink.flow.config;

/**
 * @author Lixiang
 * @description 发送模式
 */
public enum SendMode {
	/**
	 * 发送到文件
	 */
	file("file"),
	
	/**
	 * 发送到缓冲
	 */
	buffer("buffer"),
	
	/**
	 * 发送到通道
	 */
	channel("channel");
	
	public String modeName;
	
	private SendMode(String modeName) {
		this.modeName=modeName;
	}
}
