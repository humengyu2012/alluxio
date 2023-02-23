/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.web;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.StreamCache;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proxy.ProxyProcess;
import alluxio.proxy.s3.CompleteMultipartUploadHandler;
import alluxio.proxy.s3.S3RestExceptionMapper;
import alluxio.util.io.PathUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Alluxio proxy web server.
 */
@NotThreadSafe
public final class ProxyWebServer extends WebServer {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyWebServer.class);
  public static final String ALLUXIO_PROXY_SERVLET_RESOURCE_KEY = "Alluxio Proxy";
  public static final String FILE_SYSTEM_SERVLET_RESOURCE_KEY = "File System";
  public static final String STREAM_CACHE_SERVLET_RESOURCE_KEY = "Stream Cache";
  public static final String GLOBAL_RATE_LIMITER_SERVLET_RESOURCE_KEY = "Global Rate Limiter";

  public static final String SERVER_CONFIGURATION_RESOURCE_KEY = "Server Configuration";
  public static final String ALLUXIO_PROXY_AUDIT_LOG_WRITER_KEY = "Alluxio Proxy Audit Log Writer";

  public static final long MB = 1024 * 1024L;

  private final FileSystem mFileSystem;

  private final RateLimiter mGlobalRateLimiter;

  private AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;

  /**
   * Creates a new instance of {@link ProxyWebServer}.
   *
   * @param serviceName the service name
   * @param address the service address
   * @param proxyProcess the Alluxio proxy process
   */
  public ProxyWebServer(String serviceName, InetSocketAddress address,
      final ProxyProcess proxyProcess) {
    super(serviceName, address);

    // REST configuration
    ResourceConfig config = new ResourceConfig().packages("alluxio.proxy", "alluxio.proxy.s3",
            "alluxio.proxy.s3.logging")
        .register(JacksonProtobufObjectMapperProvider.class)
        .register(S3RestExceptionMapper.class);

    mFileSystem = FileSystem.Factory.create(Configuration.global());

    long rate = Configuration.getLong(PropertyKey.PROXY_S3_GLOBAL_READ_RATE_LIMIT_MB) * MB;
    if (rate <= 0) {
      rate = Long.MAX_VALUE;
    }
    this.mGlobalRateLimiter = RateLimiter.create(rate);

    if (Configuration.getBoolean(PropertyKey.PROXY_AUDIT_LOGGING_ENABLED)) {
      mAsyncAuditLogWriter = new AsyncUserAccessAuditLogWriter("PROXY_AUDIT_LOG");
      mAsyncAuditLogWriter.start();
      MetricsSystem.registerGaugeIfAbsent(
          MetricKey.PROXY_AUDIT_LOG_ENTRIES_SIZE.getName(),
              () -> mAsyncAuditLogWriter != null
                  ? mAsyncAuditLogWriter.getAuditLogEntriesSize() : -1);
    }

    ServletContainer servlet = new ServletContainer(config) {
      private static final long serialVersionUID = 7756010860672831556L;

      @Override
      public void init() throws ServletException {
        super.init();
        getServletContext().setAttribute(ALLUXIO_PROXY_SERVLET_RESOURCE_KEY, proxyProcess);
        getServletContext()
                .setAttribute(FILE_SYSTEM_SERVLET_RESOURCE_KEY, mFileSystem);
        getServletContext().setAttribute(STREAM_CACHE_SERVLET_RESOURCE_KEY,
                new StreamCache(Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS)));
        getServletContext().setAttribute(ALLUXIO_PROXY_AUDIT_LOG_WRITER_KEY, mAsyncAuditLogWriter);
        getServletContext().setAttribute(GLOBAL_RATE_LIMITER_SERVLET_RESOURCE_KEY,
            mGlobalRateLimiter);
      }

      @Override
      public void service(final ServletRequest req, final ServletResponse res)
              throws ServletException, IOException {
        Stopwatch stopWatch = Stopwatch.createStarted();
        if (!loadMetadata(req)) {
          super.service(req, res);
        }
        if ((req instanceof HttpServletRequest) && (res instanceof HttpServletResponse)) {
          HttpServletRequest httpReq = (HttpServletRequest) req;
          HttpServletResponse httpRes = (HttpServletResponse) res;
          logAccess(httpReq, httpRes, stopWatch);
        }
      }
    };
    ServletHolder servletHolder = new ServletHolder("Alluxio Proxy Web Service", servlet);
    mServletContextHandler
        .addServlet(servletHolder, PathUtils.concatPath(Constants.REST_API_PREFIX, "*"));
    // TODO(czhu): Move S3 API logging out of CompleteMultipartUploadHandler into a logging handler
    addHandler(new CompleteMultipartUploadHandler(mFileSystem, Constants.REST_API_PREFIX));
  }

  private static final String LOAD_METADATA_PATH = "/api/v1/loadMetadata";
  private static final String HTTP_METHOD_POST = "POST";
  private static final String BUCKET = "bucket";
  private static final String PATH = "path";
  private static final String FORCE = "force";
  private static final String RECURSIVE = "recursive";
  private static final int MIN_DIR_DEPTH = 4;

  /**
   *
   * @return if request is load metadata
   */
  public boolean loadMetadata(ServletRequest request) throws IOException {
    if (!(request instanceof HttpServletRequest)) {
      return false;
    }
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    String uri = httpRequest.getRequestURI();
    if (!LOAD_METADATA_PATH.equals(uri) || !HTTP_METHOD_POST.equals(httpRequest.getMethod())) {
      return false;
    }
    String bucket = Preconditions.checkNotNull(httpRequest.getParameter(BUCKET),
        "bucket can not be null");
    String path = Preconditions.checkNotNull(httpRequest.getParameter(PATH),
        "path can not be null");
    boolean force = Optional.ofNullable(httpRequest.getParameter(FORCE)).map(Boolean::parseBoolean)
        .orElse(false);
    boolean recursive = Optional.ofNullable(httpRequest.getParameter(RECURSIVE))
        .map(Boolean::parseBoolean).orElse(false);
    AlluxioURI alluxioURI = new AlluxioURI(String.format("/%s/%s", bucket, path));
    ListStatusPOptions options;
    if (force) {
      options = ListStatusPOptions.newBuilder()
          .setRecursive(recursive)
          .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
              .setSyncIntervalMs(0).build())
          .build();
    } else {
      options = ListStatusPOptions.newBuilder().setRecursive(recursive).build();
    }
    try {
      URIStatus status = mFileSystem.getStatus(alluxioURI);
      if (recursive && alluxioURI.getDepth() < MIN_DIR_DEPTH && status.isFolder()) {
        throw new IllegalArgumentException(
            "The directory is too big, the depth must less than " + MIN_DIR_DEPTH);
      }
      mFileSystem.loadMetadata(alluxioURI, options);
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
    return true;
  }

  @Override
  public void stop() throws Exception {
    if (mAsyncAuditLogWriter != null) {
      mAsyncAuditLogWriter.stop();
      mAsyncAuditLogWriter = null;
    }
    mFileSystem.close();
    super.stop();
  }

  /**
   * Log the access of every single http request.
   * @param request
   * @param response
   * @param stopWatch
   */
  public static void logAccess(HttpServletRequest request, HttpServletResponse response,
                               Stopwatch stopWatch) {
    String contentLenStr = "None";
    if (request.getHeader("x-amz-decoded-content-length") != null) {
      contentLenStr = request.getHeader("x-amz-decoded-content-length");
    } else if (request.getHeader("Content-Length") != null) {
      contentLenStr = request.getHeader("Content-Length");
    }
    String accessLog = String.format("[ACCESSLOG] Request:%s - Status:%d "
                    + "- ContentLength:%s - Elapsed(ms):%d",
            request, response.getStatus(),
            contentLenStr, stopWatch.elapsed(TimeUnit.MILLISECONDS));
    if (LOG.isDebugEnabled()) {
      String requestHeaders = Collections.list(request.getHeaderNames()).stream()
              .map(x -> x + ":" + request.getHeader(x))
              .collect(Collectors.joining("\n"));
      String responseHeaders = response.getHeaderNames().stream()
              .map(x -> x + ":" + response.getHeader(x))
              .collect(Collectors.joining("\n"));
      String moreInfoStr = String.format("%n[RequestHeader]:%n%s%n[ResponseHeader]:%n%s",
              requestHeaders, responseHeaders);
      LOG.debug(accessLog + " " + moreInfoStr);
    } else {
      LOG.info(accessLog);
    }
  }
}
