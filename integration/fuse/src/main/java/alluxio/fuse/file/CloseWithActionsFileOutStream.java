package alluxio.fuse.file;

import alluxio.client.file.FileOutStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseWithActionsFileOutStream extends FileOutStream {

  private static final Logger LOG = LoggerFactory.getLogger(CloseWithActionsFileOutStream.class);

  private final FileOutStream mFileOutStream;
  private final List<Action> mActions;

  public CloseWithActionsFileOutStream(FileOutStream fileOutStream, Action... actions) {
    this.mFileOutStream = fileOutStream;
    this.mActions = new ArrayList<>(Arrays.asList(actions));
  }

  public CloseWithActionsFileOutStream(FileOutStream fileOutStream) {
    this.mFileOutStream = fileOutStream;
    this.mActions = new ArrayList<>();
  }

  @Override
  public void write(int b) throws IOException {
    mFileOutStream.write(b);
  }

  @Override
  public long getBytesWritten() {
    return mFileOutStream.getBytesWritten();
  }

  @Override
  public void cancel() throws IOException {
    mFileOutStream.cancel();
  }

  @Override
  public void write(byte[] b) throws IOException {
    mFileOutStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mFileOutStream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    mFileOutStream.flush();
  }

  private void runAction(Action action) throws IOException {
    try {
      action.run();
    } catch (Exception e) {
      if (action.ignoreException()) {
        LOG.warn("Can not run action due to: ", e);
      } else {
        throw MemoryBufferFileOutResource.wrapAsIOException(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      for (Action action : mActions) {
        if (!action.afterClose()) {
          runAction(action);
        }
      }
    } finally {
      mFileOutStream.close();
    }
    for (Action action : mActions) {
      if (action.afterClose()) {
        runAction(action);
      }
    }
  }

  public void addAction(Action action) {
    mActions.add(action);
  }

  public interface Action {

    void run() throws Exception;

    default boolean ignoreException() {
      return true;
    }

    default boolean afterClose() {
      return true;
    }
  }

}
