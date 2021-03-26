package com.lc.plugin.sink.flow.service;

/**
 * @author Lixiang
 * @description 发送规则接口
 */
public interface SendRule {
	/**
	 * 获取流程索引表
	 * @param messageRecord 消息记录
	 * @return 流程索引表
	 */
	public int[] main(String[] flowList,String message);
}
