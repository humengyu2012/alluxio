package com.zhihu.platform.alluxio.auth.pojo;


import java.util.List;


public class Global {

  //全局ip白名单
  private List<String> whiteIpRangeList;

  public List<String> getWhiteIpRangeList() {
    return whiteIpRangeList;
  }

  public void setWhiteIpRangeList(List<String> whiteIpRangeList) {
    this.whiteIpRangeList = whiteIpRangeList;

  }

}
