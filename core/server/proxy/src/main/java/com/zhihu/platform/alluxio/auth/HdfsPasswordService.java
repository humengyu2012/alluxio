package com.zhihu.platform.alluxio.auth;

import static alluxio.conf.PropertyKey.Builder.classBuilder;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.conf.PropertyKey.ConsistencyCheckLevel;
import alluxio.grpc.Scope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.zhihu.platform.alluxio.auth.pojo.AuthInfo;
import com.zhihu.platform.alluxio.auth.pojo.User;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsPasswordService {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsPasswordService.class);
  private static HdfsPasswordService INSTANCE;
  private static final String PASSWORD_SERVICE_URL = "hdfs-password-service.url";
  public static final PropertyKey PASSWORD_SERVICE_URL_KEY =
      classBuilder(PASSWORD_SERVICE_URL)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.ALL)
          .build();

  private final URL url;
  private final Timer timer;
  private final ObjectMapper mapper;
  private Map<String, Set<String>> passwords;


  private HdfsPasswordService() {
    AlluxioConfiguration configuration = Configuration.global();
    try {
      this.url = new URL(Objects.requireNonNull(configuration.getString(PASSWORD_SERVICE_URL_KEY),
          PASSWORD_SERVICE_URL_KEY.getName() + " can not be null"));
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
    this.mapper = new ObjectMapper();
    try {
      this.passwords = readPasswords();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          passwords = readPasswords();
          LOG.info("Update passwords success");
        } catch (IOException e) {
          LOG.warn("Can not update passwords", e);
        }
      }
    }, 10 * 1000L, 10 * 1000L);
  }

  private Map<String, Set<String>> readPasswords() throws IOException {
    return mapper.readValue(url, AuthInfo.class).getUserList().stream()
        .collect(Collectors.toMap(User::getUsername, user -> new HashSet<>(user.getPasswords())));
  }

  @VisibleForTesting
  protected Map<String, Set<String>> getPasswords() {
    return passwords;
  }

  public boolean access(String username, String password) {
    return passwords.getOrDefault(username, ImmutableSet.of()).contains(password);
  }

  public static synchronized HdfsPasswordService getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new HdfsPasswordService();
    }
    return INSTANCE;
  }
}
