package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.proxy.s3.RangeFileInStream.Factory;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Merge multiple inputStream into one.
 */
public class MergedInputStream extends InputStream {

  private final LinkedList<Supplier<InputStream>> suppliers;
  private InputStream inputStream;

  public MergedInputStream(List<Supplier<InputStream>> suppliers) {
    this.suppliers = new LinkedList<>(suppliers);
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
  public int read(byte[] b, int offset, int length) throws IOException {
    if (inputStream == null && !nextInputStream()) {
      return -1;
    }
    int readBytes = 0;
    while (length > 0 && offset < b.length) {
      int read = inputStream.read(b, offset, length);
      if (read < 0) {
        if (!nextInputStream()) {
          return readBytes == 0 ? -1 : readBytes;
        } else {
          continue;
        }
      }
      offset += read;
      length -= read;
      readBytes += read;
    }
    return readBytes;
  }

  private boolean nextInputStream() throws IOException {
    while (suppliers.size() > 0) {
      // close current inputStream
      if (inputStream != null) {
        inputStream.close();
      }
      inputStream = suppliers.removeFirst().get();
      return true;
    }
    return false;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public int available() throws IOException {
    throw new UnsupportedOperationException();
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

  @Override
  public void close() throws IOException {
    if (inputStream != null) {
      inputStream.close();
    }
  }

  // TODO: make it configurable
  public static long RANGE_SIZE = Constants.GB;

  public static MergedInputStream create(FileSystem fileSystem, URIStatus status, long rangeSize) {
    AlluxioURI alluxioURI = new AlluxioURI(status.getPath());
    LinkedList<Supplier<InputStream>> suppliers = new LinkedList<>();
    long length = status.getLength();
    long offset = 0;
    while (offset < length) {
      final long start = offset;
      final long end = Math.min(start + rangeSize, length);
      suppliers.add(() -> {
        FileInStream fileInStream;
        try {
          fileInStream = fileSystem.openFile(alluxioURI);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
        try {
          S3RangeSpec s3RangeSpec = new S3RangeSpec(start, end);
          return Factory.create(fileInStream, length, s3RangeSpec);
        } catch (Exception e) {
          try {
            fileInStream.close();
          } catch (Exception ex) {
            // ignore
          }
          throw new IllegalStateException(e);
        }
      });
      offset = end;
    }
    return new MergedInputStream(suppliers);
  }

  public static MergedInputStream create(FileSystem fileSystem, URIStatus status) {
    return create(fileSystem, status, RANGE_SIZE);
  }

}
