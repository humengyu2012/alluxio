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

  private static final long CHECK_PERIOD = 1000L;
  private static final int FORWARD_BLOCK_COUNT = Math.max(Configuration.getInt(
      PropertyKey.FUSE_BLOCK_LOADER_FORWARD_BLOCK_COUNT), 1);

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
    Long pos = null;
    if (fileInStream != null) {
      pos = fileInStream.getPos();
      fileInStream.close();
    }
    fileInStream = fileSystem.openFile(uri);
    if (pos != null) {
      fileInStream.seek(pos);
    }
    // cache blocks
    blockLoader.load(fileSystem.getStatus(uri), true, true, fileInStream.getPos(),
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

