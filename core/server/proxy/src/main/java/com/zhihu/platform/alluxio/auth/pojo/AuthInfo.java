package com.zhihu.platform.alluxio.auth.pojo;


import java.util.List;

public class AuthInfo {

  private Global global;
  private List<User> userList;

  public Global getGlobal() {
    return global;
  }

  public void setGlobal(Global global) {
    this.global = global;
  }

  public List<User> getUserList() {
    return userList;
  }

  public void setUserList(List<User> userList) {
    this.userList = userList;
  }

}
