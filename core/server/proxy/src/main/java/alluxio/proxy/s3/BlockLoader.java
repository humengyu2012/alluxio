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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockLoader {

  private static final Logger LOG = LoggerFactory.getLogger(BlockLoader.class);

  private final int threadNum = Configuration.getInt(
      PropertyKey.PROXY_S3_AUTO_LOAD_ASYNC_THREAD);
  // blockId -> ts
  private final Map<Long, Long> loadingBlocks;
  private final FileSystemContext fsContext;
  private final ThreadPoolExecutor pool;
  private final Timer timer;
  private final long period;

  public BlockLoader(FileSystemContext fsContext) {
    this.fsContext = fsContext;
    this.loadingBlocks = new ConcurrentHashMap<>(64 * 1024);
    if (threadNum <= 0) {
      this.pool = null;
    } else {
      this.pool = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>());
    }
    this.timer = new Timer();
    this.period = Configuration.getLong(PropertyKey.PROXY_S3_AUTO_LOAD_CLEAR_LOADING_BLOCKS_PERIOD);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          clearLoadingBlocks();
        } catch (Exception e) {
          LOG.warn("Can not clear loading blocks", e);
        }
      }
    }, period, period);
  }

  /**
   * 十分钟清理一次 block 去重集合，这里可以用 bitmap 优化内存，用优先级队列按 ts 排序加快遍历速度
   */
  private void clearLoadingBlocks() {
    LOG.info("Start clear expired loading blocks, current loading blocks number is {}",
        loadingBlocks.size());
    long current = System.currentTimeMillis();
    for (Entry<Long, Long> entry : new HashMap<>(loadingBlocks).entrySet()) {
      long blockId = entry.getKey();
      long ts = entry.getValue();
      if (current - ts > period) {
        loadingBlocks.remove(blockId);
        LOG.info("Remove expired block: {} from loading blocks", blockId);
      }
    }
    LOG.info("Clear expired loading blocks finished, current loading blocks number is {}",
        loadingBlocks.size());
  }

  public void load(URIStatus status) {
    try {
      if (status.getInAlluxioPercentage() == 100 || status.getLength() == 0) {
        for (Long blockId : status.getBlockIds()) {
          if (loadingBlocks.remove(blockId) != null) {
            LOG.info("Remove cached block: {} from loading blocks", blockId);
          }
        }
        return;
      }
      AlluxioURI filePath = new AlluxioURI(status.getPath());
      List<Runnable> loadBlockTasks = getLoadBlockTasks(filePath, status, false, true);
      for (Runnable task : loadBlockTasks) {
        if (pool != null) {
          pool.execute(task);
        } else {
          // task 已经做了异常处理
          task.run();
        }
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
        // 及时清理，节省内存以及防止后续有 free 操作导致不能自动缓存 block
        if (loadingBlocks.remove(blockId) != null) {
          LOG.info("Remove cached block: {} from loading blocks", blockId);
        }

        continue;
      }
      if (loadingBlocks.putIfAbsent(blockId, System.currentTimeMillis()) != null) {
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
          loadingBlocks.remove(blockId);
          LOG.warn("Can not cache block: {}", blockId, e);
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


  private static final String LOCAL_NAME;

  static {
    try {
      LOCAL_NAME = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      LOG.error("Can not get local name", e);
      throw new RuntimeException(e);
    }
  }

  public static boolean allBlocksInLocal(URIStatus status) {
    List<FileBlockInfo> fileBlockInfos = status.getFileBlockInfos();
    for (FileBlockInfo fileBlockInfo : fileBlockInfos) {
      BlockInfo blockInfo = fileBlockInfo.getBlockInfo();
      if (blockInfo.getLocations().stream()
          .noneMatch(
              blockLocation -> LOCAL_NAME.equals(blockLocation.getWorkerAddress().getHost()))) {
        return false;
      }
    }
    return true;
  }

}
