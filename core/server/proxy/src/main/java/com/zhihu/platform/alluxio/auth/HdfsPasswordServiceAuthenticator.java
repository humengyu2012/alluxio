package com.zhihu.platform.alluxio.auth;

import alluxio.proxy.s3.auth.Authenticator;
import alluxio.proxy.s3.auth.AwsAuthInfo;

public class HdfsPasswordServiceAuthenticator implements Authenticator {

  @Override
  public boolean isAuthenticated(AwsAuthInfo awsAuthInfo) {
    String ak = awsAuthInfo.getAccessID();
    if (ak == null) {
      return false;
    }
    String[] split = ak.split("-");
    String username = split[0];
    String password = null;
    if (split.length >= 2) {
      password = split[1];
    }
    if (HdfsPasswordService.getInstance().access(username, password)) {
      AuthenticatorUtils.rewriteUser(awsAuthInfo, username);
      return true;
    }
    return false;
  }
}
