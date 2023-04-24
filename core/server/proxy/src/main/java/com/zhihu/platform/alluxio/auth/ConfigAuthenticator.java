package com.zhihu.platform.alluxio.auth;

import static alluxio.conf.PropertyKey.Builder.classBuilder;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.conf.PropertyKey.ConsistencyCheckLevel;
import alluxio.grpc.Scope;
import alluxio.proxy.s3.auth.Authenticator;
import alluxio.proxy.s3.auth.AwsAuthInfo;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigAuthenticator implements Authenticator {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigAuthenticator.class);

  private static final String USERNAME_PASSWORDS = "s3.proxy.authenticator.username.passwords";

  public static final PropertyKey USERNAME_PASSWORDS_KEY =
      classBuilder(USERNAME_PASSWORDS)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.ALL)
          .build();

  private static final Map<String, Set<String>> PASSWORDS = getPasswords();

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
    Set<String> values = PASSWORDS.get(username);
    if (values == null) {
      return false;
    }
    if (values.contains(password)) {
      AuthenticatorUtils.rewriteUser(awsAuthInfo, username);
      return true;
    }
    return false;
  }

  private static Map<String, Set<String>> getPasswords() {
    String value = Configuration.getString(USERNAME_PASSWORDS_KEY);
    if (value == null || value.length() == 0) {
      throw new IllegalArgumentException(USERNAME_PASSWORDS + " can not be empty");
    }
    Map<String, Set<String>> passwords = new HashMap<>();
    for (String pair : value.split(",")) {
      String[] split = pair.split(":");
      if (split.length != 2) {
        throw new IllegalArgumentException("Error format for " + USERNAME_PASSWORDS_KEY);
      }
      String username = split[0];
      String password = split[1];
      passwords.computeIfAbsent(username, k -> new HashSet<>()).add(password);
    }
    LOG.info("Current auth information is: {}", passwords);
    return passwords;
  }

}
