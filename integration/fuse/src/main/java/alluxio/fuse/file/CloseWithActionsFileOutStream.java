package alluxio.fuse.file;

import alluxio.client.file.FileOutStream;
import java.io.IOException;

public class CloseWithActionsFileOutStream extends FileOutStream {

  private final FileOutStream mFileOutStream;
  private final Runnable[] mActions;

  public CloseWithActionsFileOutStream(FileOutStream fileOutStream, Runnable... actions) {
    this.mFileOutStream = fileOutStream;
    this.mActions = actions;
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

  @Override
  public void close() throws IOException {
    mFileOutStream.close();
    if (mActions == null) {
      return;
    }
    for (Runnable action : mActions) {
      action.run();
    }
  }
}
