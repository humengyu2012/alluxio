package alluxio.fuse.file;

import alluxio.client.file.FileOutStream;
import alluxio.fuse.file.MemoryBufferFileOutResource.BytesWrap;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryBufferHdfsFileOutStream extends FileOutStream {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryBufferHdfsFileOutStream.class);

  private static final Cache<Path, Boolean> DIR_CACHE = CacheBuilder.newBuilder()
      .expireAfterWrite(1, TimeUnit.HOURS)
      .maximumSize(64 * 1024)
      .build();

  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private static final FsPermission PERMISSION = new FsPermission(FsAction.ALL, FsAction.ALL,
      FsAction.ALL);

  private final String date;
  private final DistributedFileSystem fileSystem;
  private final Path path;
  private final MemoryBufferFileOutResource resource;
  private final Path stagingDir;
  private final ThreadPoolExecutor pool;
  private final AtomicReference<Exception> exception = new AtomicReference<>();
  private final List<FutureTask<Path>> tasks = new ArrayList<>();
  private final List<Path> tmpPaths = new ArrayList<>();

  private BytesWrap currentBuffer;

  public MemoryBufferHdfsFileOutStream(DistributedFileSystem fileSystem, Path path, Path stagingDir,
      MemoryBufferFileOutResource resource) {
    this.date = LocalDate.now().format(FORMATTER);
    this.fileSystem = fileSystem;
    this.path = path;
    this.stagingDir = stagingDir;
    this.resource = resource;
    this.pool = resource.getPool();
  }

  @Override
  public long getBytesWritten() {
    return mBytesWritten;
  }

  @Override
  public void cancel() throws IOException {
    throw new IOException("The method cancel is not supported");
  }

  public Path getRandomTmpPath() {
    return
        new Path(new Path(stagingDir, date),
            MemoryBufferFileOutResource.ALLUXIO_FUSE_BLOCK + "_" + UUID.randomUUID());
  }

  private BytesWrap takeBuffer() throws IOException {
    try {
      return resource.takeBuffer();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void createDirIfNotExist(Path path) throws IOException {
    try {
      if (!DIR_CACHE.get(path, () -> fileSystem.exists(path))) {
        fileSystem.mkdirs(path, PERMISSION);
        DIR_CACHE.invalidate(path);
        // retry
        if (!DIR_CACHE.get(path, () -> fileSystem.exists(path))) {
          throw new IOException("Can not create dir " + path);
        }
      }
    } catch (ExecutionException e) {
      throw MemoryBufferFileOutResource.wrapAsIOException(e);
    }
  }

  private void writeBlock(Path blockPath, BytesWrap bytesWrap) throws IOException {
    createDirIfNotExist(blockPath.getParent());
    try (FSDataOutputStream outputStream = fileSystem.create(blockPath);
        ByteArrayInputStream inputStream = bytesWrap.snapshotAsStream()) {
      IOUtils.copyBytes(inputStream, outputStream, 4096, false);
    }
  }

  private void retry(RichRunnable runnable, int currentRetry, int maxRetry) throws Exception {
    try {
      runnable.run();
    } catch (Exception e) {
      if (currentRetry < maxRetry) {
        LOG.warn("Retry: current retry count {}, max retry count {}, due to: ", currentRetry,
            maxRetry, e);
        retry(runnable, ++currentRetry, maxRetry);
      } else {
        throw e;
      }
    }
  }

  private void writeCurrentBuffer() {
    final BytesWrap bytesWrap = currentBuffer;
    currentBuffer = null;
    Path tmpPath = getRandomTmpPath();
    tmpPaths.add(tmpPath);
    FutureTask<Path> task = new FutureTask<>(() -> {
      try {
        // 在一个 task 出现异常后，其他的 task 快速失败
        if (exception.get() != null) {
          throw exception.get();
        }
        if (bytesWrap.getSize() != 0) {
          retry(() -> writeBlock(tmpPath, bytesWrap), 0, 3);
        }
      } catch (Exception e) {
        exception.compareAndSet(null, e);
        throw e;
      } finally {
        bytesWrap.close();
      }
      return tmpPath;
    });
    pool.submit(task);
    tasks.add(task);
  }

  @Override
  public void write(int b) throws IOException {
    throw new IOException("Please invoke write(byte[] b) or write(byte[] b, int off, int len)");
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (exception.get() != null) {
      throw MemoryBufferFileOutResource.wrapAsIOException(exception.get());
    }
    if (currentBuffer == null) {
      currentBuffer = takeBuffer();
    }
    int length = Math.min(b.length - off, len);
    while (length > 0) {
      int copy = currentBuffer.put(b, off, length);
      mBytesWritten += copy;
      if (copy == 0) {
        writeCurrentBuffer();
        currentBuffer = takeBuffer();
        continue;
      }
      length -= copy;
    }
  }

  @Override
  public void flush() throws IOException {
    // do nothing
  }

  private void deleteTmpPaths() {
    for (Path tmpPath : tmpPaths) {
      try {
        fileSystem.delete(tmpPath, false);
      } catch (Exception e) {
        // ignore
      }
    }
  }

  private Path concat(List<Path> blocks) throws IOException {
    Path first = blocks.get(0);
    if (blocks.size() == 1) {
      return first;
    }
    Path[] psrcs = new Path[blocks.size() - 1];
    for (int i = 1; i < blocks.size(); i++) {
      psrcs[i - 1] = blocks.get(i);
    }
    fileSystem.concat(first, psrcs);
    return first;
  }

  @Override
  public void close() throws IOException {
    try {
      doClose();
    } catch (Exception e) {
      deleteTmpPaths();
      throw MemoryBufferFileOutResource.wrapAsIOException(e);
    } finally {
      resource.close();
    }
  }

  private void doClose() throws Exception {
    if (currentBuffer == null) {
      // 表示一次都没有写入，创建空文件即可
      fileSystem.create(path).close();
      return;
    }
    if (currentBuffer.getSize() > 0) {
      writeCurrentBuffer();
    }
    // wait for all task finished
    List<Path> blocks = new ArrayList<>();
    for (FutureTask<Path> task : tasks) {
      try {
        blocks.add(task.get());
      } catch (Exception e) {
        exception.compareAndSet(null, e);
      }
    }
    if (exception.get() != null) {
      throw exception.get();
    }
    // concat
    Path first = concat(blocks);
    fileSystem.rename(first, path, Rename.OVERWRITE);
  }

  interface RichRunnable {

    void run() throws IOException;
  }


}
