package alluxio.proxy.s3;

import alluxio.Constants;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class MergedInputStreamTest {

  private static final int DATA_SIZE = 10 * Constants.MB;

  private final Random random = new Random();

  private byte[] createRandomData() {
    byte[] data = new byte[DATA_SIZE];
    random.nextBytes(data);
    return data;
  }

  @Test
  public void testRead() throws Exception {
    for (int i = 0; i < 100; i++) {
      int rangesSize = random.nextInt(Constants.KB) + Constants.KB;
      byte[] data = createRandomData();
      List<Supplier<InputStream>> suppliers = new ArrayList<>();
      long offset = 0;
      while (offset < data.length) {
        final long finalOffset = offset;
        suppliers.add(() -> {
          ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
          inputStream.skip(finalOffset);
          return ByteStreams.limit(inputStream, rangesSize);
        });
        offset += rangesSize;
      }
      // insert empty inputStream
      for (int j = 0; j < 10; j++) {
        suppliers.add(random.nextInt(suppliers.size()),
            () -> new ByteArrayInputStream(new byte[0]));
      }
      try (MergedInputStream mergedInputStream = new MergedInputStream(suppliers);
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
        IOUtils.copy(mergedInputStream, outputStream, 1024);
        Assert.assertArrayEquals(data, outputStream.toByteArray());
      }
    }
  }
}