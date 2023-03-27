package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.client.block.BlockStoreClient;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.DeterministicHashPolicy;
import alluxio.client.block.stream.BlockInStream.BlockInStreamSource;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.OpenFilePOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerNetAddress;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockLoader {

  private static final Logger LOG = LoggerFactory.getLogger(BlockLoader.class);

  private static final int ASYNC_LOAD_BLOCK_THREAD_NUMBER = 10;

  private final Set<Long> loadingBlocks;
  private final FileSystemContext fsContext;
  private final ThreadPoolExecutor pool;

  public BlockLoader(FileSystemContext fsContext) {
    this.fsContext = fsContext;
    this.loadingBlocks = ConcurrentHashMap.newKeySet();
    this.pool = new ThreadPoolExecutor(ASYNC_LOAD_BLOCK_THREAD_NUMBER,
        ASYNC_LOAD_BLOCK_THREAD_NUMBER, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
  }

  public void load(URIStatus status) {
    try {
      if (status.getInAlluxioPercentage() == 100) {
        return;
      }
      AlluxioURI filePath = new AlluxioURI(status.getPath());
      List<Runnable> loadBlockTasks = getLoadBlockTasks(filePath, status, false, true);
      for (Runnable task : loadBlockTasks) {
        pool.execute(task);
      }
    } catch (Exception e) {
      LOG.warn("Can not load blocks", e);
    }

  }

  public List<Runnable> getLoadBlockTasks(AlluxioURI filePath, URIStatus status, boolean local,
      boolean async) {
    AlluxioConfiguration conf = fsContext.getPathConf(filePath);
    OpenFilePOptions options = FileSystemOptionsUtils.openFileDefaults(conf);
    BlockLocationPolicy policy = new DeterministicHashPolicy(conf);
    List<FileBlockInfo> fileBlockInfos = status.getFileBlockInfos();
    List<Runnable> tasks = new ArrayList<>(fileBlockInfos.size());
    for (FileBlockInfo fileBlockInfo : fileBlockInfos) {
      BlockInfo blockInfo = fileBlockInfo.getBlockInfo();
      long blockId = blockInfo.getBlockId();
      List<BlockLocation> locations = blockInfo.getLocations();
      // 代表已经有 worker 缓存了这个 block
      if (locations.size() != 0) {
        continue;
      }
      if (!loadingBlocks.add(blockId)) {
        continue;
      }
      tasks.add(() -> {
        try {
          WorkerNetAddress dataSource;
          if (local) {
            dataSource = fsContext.getNodeLocalWorker();
          } else { // send request to data source
            BlockStoreClient blockStore = BlockStoreClient.create(fsContext);
            Pair<WorkerNetAddress, BlockInStreamSource> dataSourceAndType = blockStore
                .getDataSourceAndType(blockInfo, status, policy, ImmutableMap.of());
            dataSource = dataSourceAndType.getFirst();
          }
          Protocol.OpenUfsBlockOptions openUfsBlockOptions =
              new InStreamOptions(status, options, conf, fsContext).getOpenUfsBlockOptions(blockId);
          LOG.info("Load block: blockId = {}, worker = {}", blockId, dataSource.getHost());
          cacheBlock(blockId, dataSource, status, openUfsBlockOptions, async);
        } catch (Exception e) {
          LOG.warn("Can not cache block: {}", blockId, e);
        } finally {
          loadingBlocks.remove(blockId);
        }
      });
    }
    return tasks;
  }

  private void cacheBlock(long blockId, WorkerNetAddress dataSource, URIStatus status,
      Protocol.OpenUfsBlockOptions options, boolean async) {
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
