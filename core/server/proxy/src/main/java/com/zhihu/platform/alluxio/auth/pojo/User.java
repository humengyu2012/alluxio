package com.zhihu.platform.alluxio.auth.pojo;

import java.util.List;


public class User {

  private String username;
  private boolean authEnable;
  private List<String> passwords;
  private List<String> whiteIpRangeList;


  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public List<String> getPasswords() {
    return passwords;
  }

  public void setPasswords(List<String> passwords) {
    this.passwords = passwords;
  }

  public List<String> getWhiteIpRangeList() {
    return whiteIpRangeList;
  }

  public void setWhiteIpRangeList(List<String> whiteIpRangeList) {
    this.whiteIpRangeList = whiteIpRangeList;
  }

  public boolean isAuthEnable() {
    return authEnable;
  }

  public void setAuthEnable(boolean authEnable) {
    this.authEnable = authEnable;
  }

}
