/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.fuse.auth;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.FuseFileSystem;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParentDirUserGroupAuthPolicy extends LaunchUserGroupAuthPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(
      ParentDirUserGroupAuthPolicy.class);

  private String mLoginUsername;

  /**
   * Creates a new launch user auth policy.
   *
   * @param fileSystem     file system
   * @param conf           the Alluxio configuration
   * @param fuseFileSystem fuse file system
   * @return launch user auth policy
   */
  public static ParentDirUserGroupAuthPolicy create(FileSystem fileSystem,
      AlluxioConfiguration conf, Optional<FuseFileSystem> fuseFileSystem) {
    return new ParentDirUserGroupAuthPolicy(fileSystem, fuseFileSystem);
  }

  public ParentDirUserGroupAuthPolicy(FileSystem fileSystem,
      Optional<FuseFileSystem> fuseFileSystem) {
    super(fileSystem, fuseFileSystem);
  }

  @Override
  public void init() {
    super.init();
    mLoginUsername = Configuration.getString(PropertyKey.SECURITY_LOGIN_USERNAME);
    if (mLoginUsername == null) {
      Supplier<IllegalStateException> exceptionSupplier = () -> new IllegalStateException(
          "Can not get launch username");
      mLoginUsername = AlluxioFuseUtils.getUserName(super.getUid()
          .orElseThrow(exceptionSupplier)).orElseThrow(exceptionSupplier);
    }
    LOG.info("Get fuse login user: {}", mLoginUsername);
  }

  @Override
  public void setUserGroupIfNeeded(AlluxioURI uri) {
    setUserGroupSameAsParent(uri);
  }

  @Override
  public void setUserGroup(AlluxioURI uri, long uid, long gid) {
    setUserGroupSameAsParent(uri);
  }

  private void setUserGroupSameAsParent(AlluxioURI uri) {
    try {
      if (uri.isRoot()) {
        return;
      }
      URIStatus status = mFileSystem.getStatus(uri);
      String owner = status.getOwner();
      String group = status.getGroup();
      // 这里只设置 owner 由 fuse 写入的文件，不是 fuse 写入的不做修改，
      // 判断文件是否由 fuse 写入的依据是 owner 是否等于当前登录的用户
      if (!mLoginUsername.equals(owner)) {
        LOG.info("Skip set owner group for {}, since the original owner is not {}", uri,
            mLoginUsername);
        return;
      }
      URIStatus parent = mFileSystem.getStatus(uri.getParent());
      if (Objects.equals(parent.getOwner(), owner) && Objects.equals(parent.getGroup(), group)) {
        LOG.info("Skip set owner group for {}, since the original owner group is equals to parent",
            uri);
        return;
      }
      // 这里只设置 owner，group 设置需要依赖 Alluxio master 机器上的 group
      mFileSystem.setAttribute(uri,
          SetAttributePOptions.newBuilder().setOwner(parent.getOwner()).build());
    } catch (Exception e) {
      // 在设置文件 owner 后，忽略异常，不影响正常的写入逻辑
      LOG.warn("Can not set owner/group for {}", uri, e);
    }
  }
}
