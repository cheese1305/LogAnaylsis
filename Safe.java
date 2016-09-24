package com.liqi.logmapreduce;

import java.util.regex.Pattern;

public class Safe {
	private String remote_addr; //用户的IP地址
	private String remote_user;//记录客户端的名称
	private String time_local;//记录访问时间和失去
    private String request;//记录访问的url和http协议
    private String status;//记录请求的状态
    private String body_bytes_sent;// 记录发送给客户端文件主体内容大小
    private String request_length;//请求的长度
    private String request_time;//整个请求总时间
    private String http_referer;// 用来记录从那个页面链接访问过来的
    private String http_user_agent;// 记录客户浏览器的相关信息
    @Override
	public String toString() {
		return  this.remote_user+"\t"
				+this.request+"\t"+this.status+"\t"+this.body_bytes_sent+"\t"
				+this.request_length+"\t"+this.request_time+"\t"+this.http_referer
				+"\t"+this.http_user_agent;
	}
    
    
    
    public final static String statusvaild="^\\d{3}$";
    private boolean vaild=true;//判断数据是否合法
    public  static Pattern pattern = Pattern.compile(statusvaild);
    public static Safe parser(String line){
    	Safe safe=new Safe();
    	String []arr=line.split(" ");
    	if(arr.length>11){
    		safe.setRemote_addr(arr[0]);
    		safe.setRemote_user(arr[2]);
    		safe.setTime_local(arr[3].substring(1,14).replaceFirst("T", "-"));
    		safe.setRequest(arr[5]);
    		safe.setStatus(arr[7]);
    		safe.setBody_bytes_sent(arr[8]);
    		safe.setRequest_length(arr[9]);
    		safe.setRequest_time(arr[10]);
    		safe.setHttp_referer(arr[11]);
    		safe.setHttp_user_agent(arr[12]);
    		
    		if(!pattern.matcher(safe.getStatus()).find()){
    			safe.setVaild(false);
    		}else {
    			
    		   if(Integer.parseInt(safe.getStatus()) <= 400) {// 提取所有404
    			   safe.setVaild(false);
            }
    	}
    	}else{
    		safe.setVaild(false);
    	}
		return safe;
    	
    	
    }
    
    
    
	public String getRemote_addr() {
		return remote_addr;
	}
	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}
	public String getRemote_user() {
		return remote_user;
	}
	public void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}
	public String getTime_local() {
		return time_local;
	}
	public void setTime_local(String time_local) {
		this.time_local = time_local;
	}
	public String getRequest() {
		return request;
	}
	public void setRequest(String request) {
		this.request = request;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}
	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}
	public String getRequest_length() {
		return request_length;
	}
	public void setRequest_length(String request_length) {
		this.request_length = request_length;
	}
	public String getRequest_time() {
		return request_time;
	}
	public void setRequest_time(String request_time) {
		this.request_time = request_time;
	}
	public String getHttp_referer() {
		return http_referer;
	}
	public void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}
	public String getHttp_user_agent() {
		return http_user_agent;
	}
	public void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}
	public boolean isVaild() {
		return vaild;
	}
	public void setVaild(boolean vaild) {
		this.vaild = vaild;
	}


}
