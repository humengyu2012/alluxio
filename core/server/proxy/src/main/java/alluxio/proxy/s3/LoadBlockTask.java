package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.client.block.BlockStoreClient;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.stream.BlockInStream.BlockInStreamSource;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.OpenFilePOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadBlockTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(LoadBlockTask.class);

  private final Runnable runnable;
  private boolean hasBeenRun;

  private LoadBlockTask(Runnable runnable) {
    this.runnable = runnable;
  }

  @Override
  public void run() {
    synchronized (this) {
      if (hasBeenRun) {
        return;
      }
      hasBeenRun = true;
    }
    try {
      runnable.run();
    } catch (Exception e) {
      // ignore
      LOG.warn("Can not run load block tasks");
    }
  }

  public static LinkedList<LoadBlockTask> getLoadBlockTasks(
      AlluxioURI filePath, URIStatus status, S3RangeSpec s3Range, FileSystemContext fsContext,
      boolean local, boolean async) throws IOException {
    AlluxioConfiguration conf = fsContext.getPathConf(filePath);
    OpenFilePOptions options = FileSystemOptionsUtils.openFileDefaults(conf);
    BlockLocationPolicy policy = Preconditions.checkNotNull(
        BlockLocationPolicy.Factory
            .create(conf.getClass(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY), conf),
        "UFS read location policy Required when loading files");
    WorkerNetAddress dataSource;
    List<Long> blockIds = status.getBlockIds();
    LinkedList<LoadBlockTask> loadBlockTasks = new LinkedList<>();
    long offset = s3Range.getOffset(status.getLength());
    long blockSize = status.getBlockSizeBytes();
    int startIndex = (int) (offset / blockSize);
    for (int i = startIndex; i < blockIds.size(); i++) {
      Long blockId = blockIds.get(i);

      if (local) {
        dataSource = fsContext.getNodeLocalWorker();
      } else { // send request to data source
        BlockStoreClient blockStore = BlockStoreClient.create(fsContext);
        Pair<WorkerNetAddress, BlockInStreamSource> dataSourceAndType = blockStore
            .getDataSourceAndType(status.getBlockInfo(blockId), status, policy, ImmutableMap.of());
        dataSource = dataSourceAndType.getFirst();
      }
      Protocol.OpenUfsBlockOptions openUfsBlockOptions =
          new InStreamOptions(status, options, conf, fsContext).getOpenUfsBlockOptions(blockId);
      final WorkerNetAddress finalDataSource = dataSource;
      loadBlockTasks.add(new LoadBlockTask(
          () -> cacheBlock(blockId, finalDataSource, status, openUfsBlockOptions, fsContext,
              async)));
    }
    return loadBlockTasks;
  }

  private static void cacheBlock(long blockId, WorkerNetAddress dataSource, URIStatus status,
      Protocol.OpenUfsBlockOptions options, FileSystemContext fsContext, boolean async) {
    BlockInfo info = status.getBlockInfo(blockId);
    long blockLength = info.getLength();
    String host = dataSource.getHost();
    // issues#11172: If the worker is in a container, use the container hostname
    // to establish the connection.
    if (!dataSource.getContainerHost().equals("")) {
      host = dataSource.getContainerHost();
    }
    CacheRequest request = CacheRequest.newBuilder()
        .setBlockId(blockId)
        .setLength(blockLength)
        .setOpenUfsBlockOptions(options)
        .setSourceHost(host)
        .setSourcePort(dataSource.getDataPort())
        .setAsync(async)
        .build();
    try (CloseableResource<BlockWorkerClient> blockWorker =
        fsContext.acquireBlockWorkerClient(dataSource)) {
      blockWorker.get().cache(request);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to complete cache request from %s for "
          + "block %d of file %s: %s", dataSource, blockId, status.getPath(), e), e);
    }
  }
}
