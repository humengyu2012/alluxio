package com.zhihu.platform.alluxio.auth;

import alluxio.proxy.s3.auth.AwsAuthInfo;
import java.lang.reflect.Field;

public class AuthenticatorUtils {

  private static final Field FIELD_AK;

  static {
    try {
      FIELD_AK = AwsAuthInfo.class.getDeclaredField("mAccessID");
      FIELD_AK.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  public static void rewriteUser(AwsAuthInfo awsAuthInfo, String user) {
    try {
      FIELD_AK.set(awsAuthInfo, user);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Can not rewrite user", e);
    }
  }

}
