package alluxio.fuse;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryCacheFileInStream extends FileInStream {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryCacheFileInStream.class);

  private static final long CACHE_SIZE = Configuration.getBytes(
      PropertyKey.FUSE_MEMORY_CACHE_PAGE_SIZE);
  private static final boolean AWARE_BLOCK = Configuration.getBoolean(
      PropertyKey.FUSE_MEMORY_CACHE_AWARE_BLOCK);

  private static final Cache<CacheKey, byte[]> CACHE = CacheBuilder.newBuilder()
      .maximumSize(Configuration.getInt(PropertyKey.FUSE_MEMORY_CACHE_PAGE_COUNT))
      .concurrencyLevel(Configuration.getInt(PropertyKey.FUSE_MEMORY_CACHE_CONCURRENCY_LEVEL))
      .expireAfterWrite(Configuration.getLong(PropertyKey.FUSE_MEMORY_CACHE_EXPIRE_MS),
          TimeUnit.MILLISECONDS)
      .build();

  private final FileSystem fileSystem;
  private final AlluxioURI uri;
  private final URIStatus status;
  private final FileInStream fileInStream;

  private long pos;

  public MemoryCacheFileInStream(FileSystem fileSystem, AlluxioURI uri)
      throws IOException, AlluxioException {
    this.fileSystem = fileSystem;
    this.uri = uri;
    this.status = fileSystem.getStatus(uri);
    this.fileInStream =
        AWARE_BLOCK ? new BlockAwareFileInStream(fileSystem, uri) : fileSystem.openFile(uri);
  }

  @Override
  public long remaining() {
    return status.getLength() - pos;
  }

  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int length)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void seek(long pos) throws IOException {
    this.pos = pos;
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
  public int read(byte[] b, int off, int len) throws IOException {
    int length = Math.min(len, b.length - off);
    int read = 0;
    while (length > 0) {
      byte[] bytes = getCacheBytes(length);
      if (bytes.length == 0) {
        break;
      }
      int copyLen = Math.min(length, bytes.length);
      System.arraycopy(bytes, 0, b, off + read, copyLen);
      pos += copyLen;
      read += copyLen;
      length -= copyLen;
    }
    return read == 0 ? -1 : read;
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    return super.read(buf);
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    byte[] data = new byte[len];
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
    fileInStream.close();
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

  private byte[] getCacheBytes(int length) throws IOException {
    long posKey = (pos / CACHE_SIZE) * CACHE_SIZE;
    int start = (int) (pos % CACHE_SIZE);
    try {
      if (status.getLength() <= pos) {
        return new byte[0];
      }
      byte[] bytes = CACHE.get(
          new CacheKey(uri.getPath(), status.getLastModificationTimeMs(), posKey),
          () -> {
            fileInStream.seek(posKey);
            byte[] data = new byte[(int) Math.min(CACHE_SIZE, status.getLength() - posKey)];
            IOUtils.readFully(fileInStream, data);
            return data;
          });
      return Arrays.copyOfRange(bytes, start, Math.min(bytes.length, start + length));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static class CacheKey {

    private final String path;
    private final long lastModification;
    private final long pos;

    public CacheKey(String path, long lastModification, long pos) {
      this.path = path;
      this.lastModification = lastModification;
      this.pos = pos;
    }

    public String getPath() {
      return path;
    }

    public long getLastModification() {
      return lastModification;
    }

    public long getPos() {
      return pos;
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
      return lastModification == cacheKey.lastModification && pos == cacheKey.pos
          && Objects.equal(path, cacheKey.path);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(path, lastModification, pos);
    }
  }
}
