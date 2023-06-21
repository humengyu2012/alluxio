package alluxio.fuse.file;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockAwareFileInStream extends FileInStream {

  private static final Logger LOG = LoggerFactory.getLogger(BlockAwareFileInStream.class);

  private static final long AWARE_PERIOD = Math.max(
      Configuration.getLong(PropertyKey.FUSE_MEMORY_CACHE_AWARE_BLOCK_PERIOD_MS), 1000L);

  private final FileSystem fileSystem;
  private final AlluxioURI uri;
  private boolean allBlocksInCluster;
  private FileInStream fileInStream;
  private long lastUpdateTs;

  public BlockAwareFileInStream(FileSystem fileSystem, AlluxioURI uri)
      throws IOException {
    this.fileSystem = fileSystem;
    this.uri = uri;
    checkReplaceFileInStream();
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
      if (allBlocksInCluster || System.currentTimeMillis() - lastUpdateTs < AWARE_PERIOD) {
        return;
      }
      URIStatus status = fileSystem.getStatus(uri);
      if (status.getInAlluxioPercentage() == 100) {
        allBlocksInCluster = true;
        LOG.info("The blocks of {} are all in cluster", uri);
      }
      replaceFileInStream();
      lastUpdateTs = System.currentTimeMillis();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  private synchronized void replaceFileInStream() throws IOException, AlluxioException {
    Long pos = null;
    if (fileInStream != null) {
      pos = fileInStream.getPos();
      fileInStream.close();
    }
    fileInStream = fileSystem.openFile(uri);
    if (pos != null) {
      fileInStream.seek(pos);
    }
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
