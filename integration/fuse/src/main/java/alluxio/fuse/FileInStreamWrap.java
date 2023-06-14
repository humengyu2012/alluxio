package alluxio.fuse;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class FileInStreamWrap extends FileInStream {

  private static final long CHECK_PERIOD = Math.max(
      Configuration.getLong(PropertyKey.FUSE_BLOCK_LOADER_FORWARD_BLOCK_PERIOD_MS), 1000L);
  private static final int FORWARD_BLOCK_COUNT = Math.max(Configuration.getInt(
      PropertyKey.FUSE_BLOCK_LOADER_FORWARD_BLOCK_COUNT), 1);
  private static final int WAITING_CLUSTER_CACHE_PERCENT = Configuration.getInt(
      PropertyKey.FUSE_BLOCK_LOADER_WAITING_CLUSTER_CACHE_PERCENT);
  private static final long WAITING_CLUSTER_CACHE_MAX_TIME_MS = Configuration.getLong(
      PropertyKey.FUSE_BLOCK_LOADER_WAITING_CLUSTER_CACHE_MAX_TIME_MS);
  private static final long WAITING_CLUSTER_CACHE_MIN_FILE_SIZE = Configuration.getLong(
      PropertyKey.FUSE_BLOCK_LOADER_WAITING_CLUSTER_CACHE_MIN_FILE_SIZE_BYTES);

  private final FileSystem fileSystem;
  private final AlluxioURI uri;
  private final FuseBlockLoader blockLoader;
  private boolean allInLocal;
  private FileInStream fileInStream;
  private long lastUpdateTs;

  public FileInStreamWrap(FileSystem fileSystem, AlluxioURI uri)
      throws IOException, AlluxioException {
    this.fileSystem = fileSystem;
    this.uri = uri;
    this.blockLoader = FuseBlockLoader.getInstance();
    this.lastUpdateTs = System.currentTimeMillis();
    replaceFileInStream();
  }

  @Override
  public long remaining() {
    return fileInStream.remaining();
  }

  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int length)
      throws IOException {
    checkReplaceFileInStream();
    return fileInStream.positionedRead(position, buffer, offset, length);
  }

  @Override
  public long getPos() throws IOException {
    return fileInStream.getPos();
  }

  @Override
  public void seek(long pos) throws IOException {
    fileInStream.seek(pos);
  }

  private void checkReplaceFileInStream() throws IOException {
    try {
      if (allInLocal || System.currentTimeMillis() - lastUpdateTs < CHECK_PERIOD) {
        return;
      }
      URIStatus status = fileSystem.getStatus(uri);
      if (FuseBlockLoader.allBlocksInLocal(status.getFileBlockInfos())) {
        allInLocal = true;
      }
      replaceFileInStream();
      lastUpdateTs = System.currentTimeMillis();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  private synchronized void replaceFileInStream() throws IOException, AlluxioException {
    URIStatus status = fileSystem.getStatus(uri);
    Long pos = null;
    if (fileInStream == null) {
      // 第一次读取时，需要检查集群的缓存情况
      if (status.getInAlluxioPercentage() < 100
          && status.getLength() > WAITING_CLUSTER_CACHE_MIN_FILE_SIZE) {
        FuseBlockLoader.waitingForClusterCache(fileSystem, uri, WAITING_CLUSTER_CACHE_PERCENT,
            WAITING_CLUSTER_CACHE_MAX_TIME_MS);
      }
    } else {
      pos = fileInStream.getPos();
      fileInStream.close();
    }
    fileInStream = fileSystem.openFile(uri);
    if (pos != null) {
      fileInStream.seek(pos);
    }
    // cache blocks
    blockLoader.load(status, true, true, fileInStream.getPos(),
        FORWARD_BLOCK_COUNT);
  }

  @Override
  public int read() throws IOException {
    checkReplaceFileInStream();
    return fileInStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    checkReplaceFileInStream();
    return fileInStream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkReplaceFileInStream();
    return fileInStream.read(b, off, len);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    checkReplaceFileInStream();
    return fileInStream.read(buf);
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    checkReplaceFileInStream();
    return fileInStream.read(byteBuffer, off, len);
  }

  @Override
  public void unbuffer() {
    fileInStream.unbuffer();
  }

  @Override
  public long skip(long n) throws IOException {
    return fileInStream.skip(n);
  }

  @Override
  public int available() throws IOException {
    return fileInStream.available();
  }

  @Override
  public void close() throws IOException {
    fileInStream.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    fileInStream.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    fileInStream.reset();
  }

  @Override
  public boolean markSupported() {
    return fileInStream.markSupported();
  }
}

