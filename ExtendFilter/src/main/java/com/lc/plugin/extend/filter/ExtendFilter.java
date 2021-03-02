package com.lc.plugin.extend.filter;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.FilterPluginAdapter;
import com.github.lixiang2114.script.dynamic.DyScript;
import com.lc.plugin.extend.filter.config.FilterConfig;

/**
 * @author Lixiang
 * @description 扩展过滤器
 * JAVA脚本需参照JDK1.5版本以前的规范来编写(如:不能使用foreach循环等)
 */
@SuppressWarnings("static-access")
public class ExtendFilter extends FilterPluginAdapter{
	/**
	 * 过滤器配置
	 */
	private FilterConfig filterConfig;
	
	/**
	 * 上下文类装载器 
	 */
	private ClassLoader classLoader;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ExtendFilter.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("ExtendFilter plugin starting...");
		File confFile=new File(pluginPath,"filter.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.classLoader=DyScript.geteExtendClassLoader(Thread.currentThread().getContextClassLoader());
		this.filterConfig=new FilterConfig(flow).config();
		return true;
	}

	@Override
	public Object filter(Channel<String> sourceToFilterChannel,Channel<String> filterToSinkChannel) throws Exception{
		log.info("ExtendFilter plugin filtering...");
		if(flow.filterStart) {
			log.info("ExtendFilter is already started...");
			return true;
		}
		
		flow.filterStart=true;
		String mainClass=filterConfig.mainClass;
		String mainMethod=filterConfig.MAIN_METHOD;
		try{
			String message=null;
			while(flow.filterStart) {
				if(null==(message=sourceToFilterChannel.get())) continue;
				if(0==(message=message.trim()).length()) continue;
				
				Object result=DyScript.execFunc(classLoader,mainClass,mainMethod, message);
				
				if(null==result) continue;
				String outLine=result.toString().trim();
				
				if(0==outLine.length()) continue;
				filterToSinkChannel.put(outLine);
			}
		}catch(InterruptedException e){
			log.warn("filter plugin is interrupted while waiting...");
		}
		
		return true;
	}

	@Override
	public Object stop(Object params) throws Exception {
		flow.filterStart=false;
		return true;
	}
	
	@Override
	public Object config(Object... params) throws Exception {
		log.info("ExtendFilter plugin config...");
		if(null==params || 0==params.length) return filterConfig.collectRealtimeParams();
		if(params.length<2) return filterConfig.getFieldValue((String)params[0]);
		return filterConfig.setFieldValue((String)params[0],params[1]);
	}
}
