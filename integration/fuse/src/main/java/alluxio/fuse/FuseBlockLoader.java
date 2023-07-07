package alluxio.fuse;

import alluxio.AlluxioURI;
import alluxio.client.block.BlockStoreClient;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.DeterministicHashPolicy;
import alluxio.client.block.stream.BlockInStream.BlockInStreamSource;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.fuse.file.MemoryCacheFileInStream;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.OpenFilePOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FuseBlockLoader {

  private static final Logger LOG = LoggerFactory.getLogger(FuseBlockLoader.class);

  private static final FuseBlockLoader INSTANCE = new FuseBlockLoader(FileSystemContext.create());

  private final FileSystemContext fsContext;
  // 去重集合
  private final Cache<Long, Boolean> clusterCache;
  private final Cache<Long, Boolean> localCache;
  private final long autoLoadMinFileSize;

  private FuseBlockLoader(FileSystemContext fsContext) {
    this.fsContext = fsContext;
    this.clusterCache = CacheBuilder.newBuilder()
        .expireAfterWrite(Configuration.getLong(PropertyKey.FUSE_BLOCK_LOADER_DEDUPLICATE_MS),
            TimeUnit.MILLISECONDS)
        .maximumSize(128 * 1024)
        .initialCapacity(8 * 1024)
        .build();
    this.localCache = CacheBuilder.newBuilder()
        .expireAfterWrite(Configuration.getLong(PropertyKey.FUSE_BLOCK_LOADER_DEDUPLICATE_MS),
            TimeUnit.MILLISECONDS)
        .maximumSize(128 * 1024)
        .initialCapacity(8 * 1024)
        .build();
    this.autoLoadMinFileSize = Configuration.getBytes(PropertyKey.FUSE_AUTO_LOAD_MIN_FILE_SIZE);
    LOG.info("Auto load min file size is set to: {} bytes", autoLoadMinFileSize);
  }

  public void load(URIStatus status, boolean local, boolean async, long startOffset,
      int cacheBlockCount) {
    if (status.getInAlluxioPercentage() == 100) {
      LOG.info("Skip load {} to cluster, since all blocks are already in alluxio cluster",
          status.getPath());
      return;
    }
    if (status.getLength() < autoLoadMinFileSize) {
      LOG.info("Skip load {} to cluster, since file size {} < {}", status.getPath(),
          status.getLength(), autoLoadMinFileSize);
      return;
    }
    LOG.info("Load {} to cluster", status.getPath());
    try {
      AlluxioURI filePath = new AlluxioURI(status.getPath());
      List<Runnable> loadBlockTasks = getLoadBlockTasks(filePath, status, local, async, startOffset,
          cacheBlockCount);
      for (Runnable task : loadBlockTasks) {
        task.run();
      }
    } catch (Exception e) {
      LOG.warn("Can not load blocks", e);
    }
  }

  private List<Runnable> getLoadBlockTasks(AlluxioURI filePath, URIStatus status, boolean local,
      boolean async, long startOffset, int cacheBlockCount) {
    Preconditions.checkArgument(startOffset >= 0, "'startOffset >= 0' is not satisfied");
    Preconditions.checkArgument(cacheBlockCount > 0, "'cacheBlockCount > 0' is not satisfied");

    AlluxioConfiguration conf = fsContext.getPathConf(filePath);
    OpenFilePOptions options = FileSystemOptionsUtils.openFileDefaults(conf);
    // find blocks to cache
    List<FileBlockInfo> fileBlockInfos = status.getFileBlockInfos().stream()
        .filter(
            fileBlockInfo -> fileBlockInfo.getOffset() + fileBlockInfo.getBlockInfo().getLength()
                >= startOffset)
        .limit(cacheBlockCount).collect(Collectors.toList());
    List<Runnable> tasks = new ArrayList<>(fileBlockInfos.size());
    BlockLocationPolicy policy = new DeterministicHashPolicy(conf);
    for (FileBlockInfo fileBlockInfo : fileBlockInfos) {
      BlockInfo blockInfo = fileBlockInfo.getBlockInfo();
      long blockId = blockInfo.getBlockId();

      List<BlockLocation> locations = blockInfo.getLocations();
      // 代表已经有 worker 缓存了这个 block
      if (locations.size() != 0) {
        // 在已经有缓存的情况下，如果不是强制缓存到 local，就不用缓存了
        if (!local) {
          continue;
        }
        // 在强制缓存到 local 的情况下，需要检查一下 local 是不是有这个缓存，有就不缓存了
        if (blockInLocal(fileBlockInfo)) {
          continue;
        }
      }

      // 查看去重集合里是否已经有这个 block 了
      if (local) {
        if (localCache.getIfPresent(blockId) != null) {
          continue;
        }
        localCache.put(blockId, true);
        LOG.info("Put {} to local cache", blockId);
      } else {
        if (clusterCache.getIfPresent(blockId) != null) {
          continue;
        }
        clusterCache.put(blockId, true);
        LOG.info("Put {} to cluster cache", blockId);
      }
      tasks.add(() -> {
        try {
          WorkerNetAddress dataSource;
          if (local) {
            dataSource = fsContext.getNodeLocalWorkerV2();
            if (dataSource == null) {
              localCache.invalidate(blockId);
              LOG.info("Skip loading block {} to local, because no local worker is found", blockId);
              return;
            }
          } else {
            // send request to data source
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
          // 缓存 block 失败后，移除去重逻辑
          if (local) {
            localCache.invalidate(blockId);
          } else {
            clusterCache.invalidate(blockId);
          }
          LOG.info("Remove {} from cache", blockId);
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
  private static final String LOCAL_IP;

  static {
    try {
      InetAddress localHost = InetAddress.getLocalHost();
      LOCAL_NAME = localHost.getCanonicalHostName();
      LOCAL_IP = localHost.getHostAddress();
      LOG.info("Local name is " + LOCAL_NAME);
      LOG.info("Local ip is " + LOCAL_IP);
    } catch (UnknownHostException e) {
      LOG.error("Can not get local name/ip", e);
      throw new RuntimeException(e);
    }
  }

  public static boolean blockInLocal(FileBlockInfo fileBlockInfo) {
    return fileBlockInfo.getBlockInfo().getLocations().stream()
        .anyMatch(blockLocation -> isLocalWorker(blockLocation.getWorkerAddress()));
  }

  public static boolean isLocalWorker(WorkerNetAddress workerAddress) {
    return LOCAL_NAME.equals(workerAddress.getHost())
        || LOCAL_NAME.equals(workerAddress.getContainerHost())
        || LOCAL_IP.equals(workerAddress.getHost())
        || LOCAL_IP.equals(workerAddress.getContainerHost());
  }

  public static boolean allBlocksInLocal(List<FileBlockInfo> fileBlockInfos) {
    return fileBlockInfos.stream().allMatch(FuseBlockLoader::blockInLocal);
  }

  private static final boolean MEMORY_CACHE_ENABLE = Configuration.getBoolean(
      PropertyKey.FUSE_MEMORY_CACHE_ENABLE);
  private static final boolean AUTO_LOAD_ENABLE = Configuration.getBoolean(
      PropertyKey.FUSE_AUTO_LOAD_ENABLE);

  public static FileInStream create(FileSystem fileSystem, AlluxioURI uri)
      throws IOException, AlluxioException {
    URIStatus status = fileSystem.getStatus(uri);
    if (AUTO_LOAD_ENABLE) {
      INSTANCE.load(status, false, true, 0, Integer.MAX_VALUE);
    }
    if (MEMORY_CACHE_ENABLE) {
      return new MemoryCacheFileInStream(fileSystem, status);
    }
    return fileSystem.openFile(uri);
  }

  public static FuseBlockLoader getInstance() {
    return INSTANCE;
  }

}

