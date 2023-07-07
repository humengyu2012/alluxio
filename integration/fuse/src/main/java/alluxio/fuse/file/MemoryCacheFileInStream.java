package alluxio.fuse.file;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.metrics.MetricsSystem;
import com.codahale.metrics.Counter;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryCacheFileInStream extends FileInStream {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryCacheFileInStream.class);

  private static final String READ_FROM_WORKER_METRICS_NAME = "Fuse_memory_cache_read_from_worker";
  private static final Counter READ_FROM_WORKER_COUNTER = MetricsSystem.counter(
      READ_FROM_WORKER_METRICS_NAME);

  private static final String READ_FROM_CACHE_METRICS_NAME = "Fuse_memory_cache_read_from_cache";
  private static final Counter READ_FROM_CACHE_COUNTER = MetricsSystem.counter(
      READ_FROM_CACHE_METRICS_NAME);


  private static final boolean AWARE_BLOCK;
  private static final long AWARE_BLOCK_MIN_FILE_SIZE;
  private static final int PAGE_SIZE;
  private static final Cache<CacheKey, byte[]> CACHE;

  static {
    AWARE_BLOCK = Configuration.getBoolean(PropertyKey.FUSE_MEMORY_CACHE_AWARE_BLOCK);
    AWARE_BLOCK_MIN_FILE_SIZE = Configuration.getBytes(
        PropertyKey.FUSE_MEMORY_CACHE_AWARE_BLOCK_MIN_FILE_SIZE);
    LOG.info("Aware block is set to: {}, min file size: {} bytes", AWARE_BLOCK,
        AWARE_BLOCK_MIN_FILE_SIZE);

    long pageSize = Configuration.getBytes(PropertyKey.FUSE_MEMORY_CACHE_PAGE_SIZE);
    Preconditions.checkArgument(pageSize <= Integer.MAX_VALUE,
        PropertyKey.FUSE_MEMORY_CACHE_PAGE_SIZE.getName() + " must be less than "
            + Integer.MAX_VALUE);
    PAGE_SIZE = (int) pageSize;
    LOG.info("The page size of memory cache is set to: " + PAGE_SIZE);

    int pageCount = Configuration.getInt(PropertyKey.FUSE_MEMORY_CACHE_PAGE_COUNT);
    Preconditions.checkArgument(pageCount > 0,
        PropertyKey.FUSE_MEMORY_CACHE_PAGE_COUNT.getName() + " must be greater than 0");

    int concurrencyLevel = Configuration.getInt(PropertyKey.FUSE_MEMORY_CACHE_CONCURRENCY_LEVEL);
    Preconditions.checkArgument(concurrencyLevel > 0,
        PropertyKey.FUSE_MEMORY_CACHE_CONCURRENCY_LEVEL.getName() + " must be greater than 0");

    long expireMs = Configuration.getLong(PropertyKey.FUSE_MEMORY_CACHE_EXPIRE_MS);
    Preconditions.checkArgument(expireMs > 0,
        PropertyKey.FUSE_MEMORY_CACHE_EXPIRE_MS.getName() + " must be greater than 0");

    LOG.info("Building memory cache with pageCount = {}, concurrencyLevel = {}, expireMs = {}",
        pageCount, concurrencyLevel, expireMs);
    CACHE = CacheBuilder.newBuilder()
        .maximumSize(pageCount)
        .concurrencyLevel(concurrencyLevel)
        .expireAfterWrite(expireMs, TimeUnit.MILLISECONDS)
        .build();
  }

  private final FileSystem fileSystem;
  private final AlluxioURI uri;
  private final URIStatus mStatus;
  private final FileInStream mFileInStream;

  private long mPos;

  public MemoryCacheFileInStream(FileSystem fileSystem, URIStatus status)
      throws IOException, AlluxioException {
    this.fileSystem = fileSystem;
    this.mStatus = status;
    this.uri = new AlluxioURI(status.getPath());
    this.mFileInStream =
        AWARE_BLOCK && mStatus.getInAlluxioPercentage() != 100
            && mStatus.getLength() >= AWARE_BLOCK_MIN_FILE_SIZE ? new BlockAwareFileInStream(
            fileSystem, uri) : fileSystem.openFile(uri);
  }

  @Override
  public long remaining() {
    return mStatus.getLength() - mPos;
  }

  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int length)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getPos() throws IOException {
    return mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    mPos = pos;
  }

  @Override
  public int read() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    int length = Math.min(len, b.length - off);
    int offset = off;
    int read = 0;
    while (length > 0) {
      int copyLen = copyCacheBytes(b, offset, length);
      if (copyLen <= 0) {
        break;
      }
      mPos += copyLen;
      read += copyLen;
      length -= copyLen;
      offset += copyLen;
    }
    if (read > 0) {
      READ_FROM_CACHE_COUNTER.inc(read);
    }
    return read == 0 ? -1 : read;
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    return super.read(buf);
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    byteBuffer.position(off);
    int length = Math.min(len, byteBuffer.capacity() - off);
    byte[] data = new byte[length];
    int read = read(data);
    if (read > 0) {
      byteBuffer.put(data, 0, read);
    }
    return read;
  }

  @Override
  public void unbuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long skip(long n) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int available() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    mFileInStream.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void reset() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean markSupported() {
    throw new UnsupportedOperationException();
  }

  private int copyCacheBytes(byte[] dest, int offset, int length) throws IOException {
    int start = (int) (mPos % PAGE_SIZE);
    long posKey = mPos - start;
    try {
      if (mStatus.getLength() <= mPos) {
        return -1;
      }
      byte[] src = CACHE.get(
          new CacheKey(mStatus.getPath(), mStatus.getLastModificationTimeMs(), posKey),
          () -> {
            mFileInStream.seek(posKey);
            byte[] data = new byte[(int) Math.min(PAGE_SIZE, mStatus.getLength() - posKey)];
            IOUtils.readFully(mFileInStream, data);
            READ_FROM_WORKER_COUNTER.inc(data.length);
            return data;
          });
      int read = Math.min(src.length - start, length);
      read = Math.min(read, dest.length - offset);
      System.arraycopy(src, start, dest, offset, read);
      return read;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * A class used as a cache key.
   */
  public static class CacheKey {

    private final String mPath;
    private final long mLastModification;
    private final long mPos;

    /**
     * @param path
     * @param lastModification
     * @param pos
     */
    public CacheKey(String path, long lastModification, long pos) {
      mPath = path;
      mLastModification = lastModification;
      mPos = pos;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return mLastModification == cacheKey.mLastModification && mPos == cacheKey.mPos
          && Objects.equal(mPath, cacheKey.mPath);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mPath, mLastModification, mPos);
    }
  }
}
