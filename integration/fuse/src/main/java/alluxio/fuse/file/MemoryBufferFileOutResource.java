package alluxio.fuse.file;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.fuse.file.CloseWithActionsFileOutStream.Action;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.metrics.MetricsSystem;
import com.codahale.metrics.Counter;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryBufferFileOutResource implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryBufferHdfsFileOutStream.class);

  private static final Counter OUTPUT_STREAM_NUMBER_TOTAL = MetricsSystem.counter(
      "Fuse_memory_buffer_output_stream_number_total");
  private static final Counter OUTPUT_STREAM_NUMBER_IN_USE = MetricsSystem.counter(
      "Fuse_memory_buffer_output_stream_number_in_use");
  private static final Counter BUFFER_NUMBER_TOTAL = MetricsSystem.counter(
      "Fuse_memory_buffer_output_stream_buffer_number_total");
  private static final Counter BUFFER_NUMBER_IN_USE = MetricsSystem.counter(
      "Fuse_memory_buffer_output_stream_buffer_number_in_use");


  public static final String ALLUXIO_FUSE_STAGING = ".alluxio_fuse_staging";
  public static final String ALLUXIO_FUSE_BLOCK = ".alluxio_fuse_block";
  public static final Pattern USER_DIR_PATTERN = Pattern.compile("(/user/[^/]+)/.+");

  private static final int OUTPUT_STREAM_NUMBER = Configuration.getInt(
      PropertyKey.FUSE_MEMORY_BUFFER_OUTPUT_STREAM_NUMBER);
  private static final int PARALLELISM = Configuration.getInt(
      PropertyKey.FUSE_MEMORY_BUFFER_PARALLELISM);
  private static final long BLOCK_CLEAN_PERIOD_MS = Configuration.getMs(
      PropertyKey.FUSE_MEMORY_BUFFER_CLEAN_BLOCK_FILE_PERIOD);
  private static final long BLOCK_EXPIRED_MS = Configuration.getMs(
      PropertyKey.FUSE_MEMORY_BUFFER_TMP_BLOCK_EXPIRED_TIME);

  private static final int BLOCK_SIZE = (int) Math.min(
      Configuration.getBytes(PropertyKey.FUSE_MEMORY_BUFFER_BLOCK_SIZE), Integer.MAX_VALUE);

  private static final Map<Path, DistributedFileSystem> ALLUXIO_FUSE_STAGING_DIRS =
      new ConcurrentHashMap<>();

  private static ArrayBlockingQueue<MemoryBufferFileOutResource> RESOURCES;
  private static ThreadPoolExecutor POOL;
  private static ArrayBlockingQueue<BytesWrap> BUFFERS;

  static {
    if (OUTPUT_STREAM_NUMBER > 0) {
      RESOURCES = new ArrayBlockingQueue<>(OUTPUT_STREAM_NUMBER);
      for (int i = 0; i < OUTPUT_STREAM_NUMBER; i++) {
        try {
          OUTPUT_STREAM_NUMBER_TOTAL.inc();
          RESOURCES.put(new MemoryBufferFileOutResource());
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      POOL = new ThreadPoolExecutor(PARALLELISM, PARALLELISM, 0,
          TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
          (r, executor) -> {
            try {
              executor.getQueue().put(r);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
      BUFFERS = new ArrayBlockingQueue<>(PARALLELISM);
      for (int i = 0; i < PARALLELISM; i++) {
        BUFFERS.add(new BytesWrap(new byte[BLOCK_SIZE]));
        BUFFER_NUMBER_TOTAL.inc();
      }

      LOG.info("Block clean config: period = {}ms, expired = {}ms", BLOCK_CLEAN_PERIOD_MS,
          BLOCK_EXPIRED_MS);
      Executors.newScheduledThreadPool(1)
          .scheduleWithFixedDelay(() -> {
            try {
              LOG.info("Start clean block files");
              cleanBlockFiles();
            } catch (Exception e) {
              LOG.warn("Can not clean block files due to:", e);
            }
          }, BLOCK_CLEAN_PERIOD_MS, BLOCK_CLEAN_PERIOD_MS, TimeUnit.MILLISECONDS);
    }
  }

  public static Optional<MemoryBufferFileOutResource> getResource() {
    MemoryBufferFileOutResource value = RESOURCES.poll();
    if (value != null) {
      OUTPUT_STREAM_NUMBER_IN_USE.inc();
    }
    return Optional.ofNullable(value);
  }

  private boolean closed;

  public ThreadPoolExecutor getPool() {
    return POOL;
  }

  public BytesWrap takeBuffer() throws InterruptedException {
    BytesWrap bytesWrap = BUFFERS.take();
    BUFFER_NUMBER_IN_USE.inc();
    return bytesWrap;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (RESOURCES.offer(new MemoryBufferFileOutResource())) {
      OUTPUT_STREAM_NUMBER_IN_USE.dec();
    }
  }

  public static boolean isAlluxioFuseStagingDir(Path path) {
    return path.getName().startsWith(ALLUXIO_FUSE_STAGING);
  }

  public static void addStagingDir(Path path, DistributedFileSystem fileSystem) {
    if (ALLUXIO_FUSE_STAGING_DIRS.putIfAbsent(path, fileSystem) == null) {
      LOG.info("Add {} to alluxio fuse staging dir", path);
    }
  }

  public static void cleanBlockFiles() {
    for (Entry<Path, DistributedFileSystem> entry : new HashMap<>(
        ALLUXIO_FUSE_STAGING_DIRS).entrySet()) {
      Path stagingDir = entry.getKey();
      DistributedFileSystem fileSystem = entry.getValue();
      try {
        if (!isAlluxioFuseStagingDir(stagingDir)) {
          LOG.info("{} is not fuse staging dir, remove it", stagingDir);
          ALLUXIO_FUSE_STAGING_DIRS.remove(stagingDir);
          continue;
        }
        if (!fileSystem.exists(stagingDir)) {
          LOG.info("{} does not exist, remove it", stagingDir);
          ALLUXIO_FUSE_STAGING_DIRS.remove(stagingDir);
          continue;
        }
        LOG.info("Start clean block files for {}", stagingDir);
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(stagingDir, true);
        while (iterator.hasNext()) {
          LocatedFileStatus fileStatus = iterator.next();
          Path path = fileStatus.getPath();
          if (path.getName().startsWith(ALLUXIO_FUSE_BLOCK)
              && System.currentTimeMillis() - fileStatus.getModificationTime() > BLOCK_EXPIRED_MS) {
            LOG.info("Delete {}", path);
            fileSystem.delete(path, false);
          }
        }
      } catch (Exception e) {
        LOG.warn("Can not clean block files in {}", stagingDir, e);
      }
    }
  }

  public static IOException wrapAsIOException(Exception e) {
    if (e instanceof IOException) {
      return (IOException) e;
    }
    return new IOException(e);
  }

  public static Optional<Path> getAlluxioStagingDir(Path path) {
    Matcher matcher = USER_DIR_PATTERN.matcher(path.toUri().getPath());
    if (!matcher.find()) {
      return Optional.empty();
    }
    return Optional.of(new Path(matcher.group(1), ALLUXIO_FUSE_STAGING));
  }

  public static boolean isEnabled() {
    return OUTPUT_STREAM_NUMBER > 0;
  }

  public static final String HDFS_PREFIX = "hdfs://";
  public static final org.apache.hadoop.conf.Configuration HADOOP_CONF = new org.apache.hadoop.conf.Configuration();

  static {
    HADOOP_CONF.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "000");
  }

  public static CloseWithActionsFileOutStream createFileOutStream(FileSystem fileSystem,
      AlluxioURI uri, CreateFilePOptions options) throws IOException, AlluxioException {
    if (!MemoryBufferFileOutResource.isEnabled()) {
      return new CloseWithActionsFileOutStream(fileSystem.createFile(uri, options));
    }
    // get parent
    URIStatus parent = fileSystem.getStatus(uri.getParent());
    String ufsPathParent = parent.getUfsPath();
    if (!ufsPathParent.startsWith(HDFS_PREFIX)) {
      return new CloseWithActionsFileOutStream(fileSystem.createFile(uri, options));
    }
    Path path = new Path(ufsPathParent, uri.getName());
    Optional<Path> stagingDir = getAlluxioStagingDir(path);
    if (!stagingDir.isPresent()) {
      return new CloseWithActionsFileOutStream(fileSystem.createFile(uri, options));
    }
    Optional<MemoryBufferFileOutResource> resource = getResource();
    if (!resource.isPresent()) {
      return new CloseWithActionsFileOutStream(fileSystem.createFile(uri, options));
    }
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) path.getFileSystem(HADOOP_CONF);
      LOG.info("Use MemoryBufferHdfsFileOutStream for {}", uri);
      Action action = () -> fileSystem.loadMetadata(uri, ListStatusPOptions.newBuilder()
          .setRecursive(false)
          .setCommonOptions(
              FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build())
          .build());
      MemoryBufferFileOutResource.addStagingDir(stagingDir.get(), dfs);
      return new CloseWithActionsFileOutStream(
          new MemoryBufferHdfsFileOutStream(dfs, path, stagingDir.get(), resource.get()), action);
    } catch (Exception e) {
      resource.get().close();
      throw e;
    }
  }


  /**
   * 后续可以换成 bytebuffer 之类的工具，用堆外内存也行
   */
  public static class BytesWrap implements Closeable {

    private final byte[] bytes;
    private int size;
    private boolean closed;

    public BytesWrap(byte[] bytes) {
      this.bytes = bytes;
      this.size = 0;
    }

    public synchronized int getSize() {
      checkClosed();
      return size;
    }

    public synchronized boolean isFull() {
      checkClosed();
      return size == bytes.length;
    }

    public synchronized int put(byte[] data, int offset, int length) {
      checkClosed();
      if (isFull()) {
        return 0;
      }
      int copy = Math.min(length, data.length - offset);
      copy = Math.min(bytes.length - size, copy);
      System.arraycopy(data, offset, bytes, size, copy);
      size += copy;
      return copy;
    }

    private void checkClosed() {
      if (closed) {
        throw new IllegalStateException("BytesWrap is closed");
      }
    }

    public synchronized ByteArrayInputStream snapshotAsStream() {
      checkClosed();
      return new ByteArrayInputStream(bytes, 0, size);
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        return;
      }
      this.closed = true;
      if (BUFFERS.offer(new BytesWrap(bytes))) {
        BUFFER_NUMBER_IN_USE.dec();
      }
    }
  }
}
