package alluxio.proxy.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

public class LoadBlockInputStream extends InputStream {

  private final LinkedList<LoadBlockTask> loadBlockTasks;
  private final long blockSize;
  private final InputStream mInputStream;
  private final int preLoadBlockCount;
  private long count;

  public LoadBlockInputStream(LinkedList<LoadBlockTask> loadBlockTasks, long blockSize,
      InputStream mInputStream, int preLoadBlockCount) {
    this.loadBlockTasks = loadBlockTasks;
    this.blockSize = blockSize;
    this.mInputStream = mInputStream;
    this.preLoadBlockCount = preLoadBlockCount;
    loadFirstBatch();
  }


  @Override
  public int read() throws IOException {
    int read = mInputStream.read();
    addCountAndLoadNext(read > 0 ? 1 : 0);
    return read;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int read = mInputStream.read(b, off, len);
    addCountAndLoadNext(read);
    return read;
  }

  @Override
  public void close() throws IOException {
    mInputStream.close();
  }

  private void addCountAndLoadNext(int read) {
    if (read > 0) {
      count += read;
      if (count % blockSize == 0) {
        loadNext();
      }
    }
  }

  private void loadFirstBatch() {
    for (int i = 1; i <= preLoadBlockCount; i++) {
      if (loadBlockTasks.size() == 0) {
        return;
      }
      loadBlockTasks.removeFirst().run();
    }
  }

  private void loadNext() {
    if (loadBlockTasks.size() == 0) {
      return;
    }
    loadBlockTasks.removeFirst().run();
  }

}
