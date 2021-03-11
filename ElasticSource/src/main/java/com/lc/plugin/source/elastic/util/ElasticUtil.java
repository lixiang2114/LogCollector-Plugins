package com.lc.plugin.source.elastic.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description Elastic工具
 */
@SuppressWarnings({"unused","rawtypes","unchecked"})
public class ElasticUtil {
	/**
	 * ES集群客户端
	 */
	private RestClient restClient;
	
	public ElasticUtil(RestClient restClient) {
		this.restClient=restClient;
	}
	
	/**
	 * 提交文档对象到ES服务
	 * @param indexAndType 索引及类型(以'/'开头)
	 * @param docId 文档ID(以'/'开头)
	 * @param document 文档对象 
	 */
	public void push(String indexAndType,String docId,Object document) {
		push(indexAndType+docId,document);
	}
	
	/**
	 * 提交文档对象到ES服务
	 * @param indexName 索引名称(以'/'开头)
	 * @param indexType 索引类型(以'/'开头)
	 * @param docId 文档ID(以'/'开头)
	 * @param document 文档对象 
	 */
	public void push(String indexName,String indexType,String docId,Object document) {
		push(indexName+indexType+docId,document);
	}
	
	/**
	 * 提交文档对象到ES服务
	 * @param docKey 文档键(以'/'开头)
	 * @param docId 文档ID
	 * @param document 文档对象 
	 */
	public void push(String docKey,Object document) {
		executePost(docKey+"?pretty",document,new BasicHeader("Content-Type","application/json;charset=UTF-8"));
	}
	
	/**
	 * 发起HTTP请求
	 * @param indexName 索引库名
	 * @param queryParam 查询参数(实体或字典)
	 * @return 响应对象
	 */
	public ArrayList<HashMap> pull(String indexName,Object queryParam) {
		Response response=executeGet(indexName+"/_search",queryParam,new BasicHeader("Content-Type","application/json;charset=UTF-8"));
		
		HttpEntity entity=null;
		if(null==response || null==(entity=response.getEntity())) return null;
		
		String responseBody=null;
		try {
			responseBody=EntityUtils.toString(entity);
		} catch (ParseException | IOException e) {
			e.printStackTrace();
		}
		
		return null==responseBody?null:(ArrayList<HashMap>)CommonUtil.getOgnlValue(responseBody, "hits.hits");
	}
	
	/**
	 * 发起HTTP请求
	 * @param uri 请求路径
	 * @param msgBody 消息体对象
	 * @param headers 头域列表
	 * @return 响应对象
	 */
	private Response executeGet(String uri,Object msgBody,Header... headers) {
		return executeRequest(uri,"GET",null,msgBody,headers);
	}
	
	/**
	 * 发起HTTP请求
	 * @param uri 请求路径
	 * @param msgBody 消息体对象
	 * @param headers 头域列表
	 * @return 响应对象
	 */
	private Response executePost(String uri,Object msgBody,Header... headers) {
		return executeRequest(uri,"POST",null,msgBody,headers);
	}
	
	/**
	 * 发起HTTP请求
	 * @param uri 请求路径
	 * @param queryString 查询字串
	 * @param msgBody 消息体对象
	 * @param headers 头域列表
	 * @return 响应对象
	 */
	private Response executeRequest(String uri,Map<String,String> queryString,Object msgBody,Header... headers) {
		return executeRequest(uri,"POST",queryString,msgBody,headers);
	}
	
	/**
	 * 发起HTTP请求
	 * @param uri 请求路径
	 * @param method 请求方法
	 * @param queryString 查询字串
	 * @param msgBody 消息体对象
	 * @param headers 头域列表
	 * @return 响应对象
	 */
	private Response executeRequest(String uri,String method,Map<String,String> queryString,Object msgBody,Header... headers) {
		if(null==uri || 0==uri.trim().length()) return null;
		if(null==method || 0==method.trim().length()) method="POST";
		
		Request request=new Request(method,uri);
		if(null!=queryString && 0!=queryString.size()) request.addParameters(queryString);
		if(null!=headers && 0!=headers.length) addHeader(request,headers);
		if(null!=msgBody) request.setEntity(new StringEntity(CommonUtil.javaToJsonStr(msgBody),ContentType.APPLICATION_JSON));
		
		try {
			return restClient.performRequest(request);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * 添加请求头
	 * @param request 请求对象
	 * @param headers 头域列表
	 */
	private void addHeader(Request request,Header... headers) {
		RequestOptions.Builder builder=request.getOptions().toBuilder();
		for(Header header:headers) builder.addHeader(header.getName(), header.getValue());
		 request.setOptions(builder);
	}
}
