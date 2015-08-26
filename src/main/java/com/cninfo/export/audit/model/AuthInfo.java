package com.cninfo.export.audit.model;

/**
 * 
 * 存储数据库连接信息的javaBean
 * 
 * @author chenyusong
 * Create Date： 2015年7月10日
 * @version 1.0.0
 *
 */
public class AuthInfo {
	
	private String host;
	private int port;
	private String user;
	private String password;
	
	private String sid;
	
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getSid() {
		return sid;
	}
	public void setSid(String sid) {
		this.sid = sid;
	}

	public String toString(){
		return user+"/"+password+"@"+host+":"+port+"/"+sid;
	}
}
