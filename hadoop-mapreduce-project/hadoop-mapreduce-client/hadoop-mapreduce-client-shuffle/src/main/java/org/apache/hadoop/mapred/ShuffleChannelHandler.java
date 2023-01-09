package org.apache.hadoop.mapred;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.SecretKey;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.task.reduce.ShuffleHeader;
import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.apache.hadoop.util.DiskChecker;
import org.eclipse.jetty.http.HttpHeader;
import org.slf4j.LoggerFactory;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderNames.TRANSFER_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderValues.CHUNKED;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.mapred.ShuffleHandler.AUDITLOG;
import static org.apache.hadoop.mapred.ShuffleHandler.CONNECTION_CLOSE;
import static org.apache.hadoop.mapred.ShuffleHandler.FETCH_RETRY_DELAY;
import static org.apache.hadoop.mapred.ShuffleHandler.IGNORABLE_ERROR_MESSAGE;
import static org.apache.hadoop.mapred.ShuffleHandler.RETRY_AFTER_HEADER;
import static org.apache.hadoop.mapred.ShuffleHandler.TIMEOUT_HANDLER;
import static org.apache.hadoop.mapred.ShuffleHandler.TOO_MANY_REQ_STATUS;

/**
 * A simple handler that serves incoming HTTP requests to send their respective
 * HTTP responses. The HTTP response uses chunked transfer encoding to send the data.
 * TODO: better documentation.
 *
 * <pre>
 * Request Headers
 * ===================
 * GET /mapOutput?job=job_1672901779104_0001&reduce=0&map=attempt_1672901779104_0001_m_000003_0,attempt_1672901779104_0001_m_000002_0,attempt_1672901779104_0001_m_000001_0,attempt_1672901779104_0001_m_000000_0,attempt_1672901779104_0001_m_000005_0,attempt_1672901779104_0001_m_000012_0,attempt_1672901779104_0001_m_000009_0,attempt_1672901779104_0001_m_000010_0,attempt_1672901779104_0001_m_000007_0,attempt_1672901779104_0001_m_000011_0,attempt_1672901779104_0001_m_000008_0,attempt_1672901779104_0001_m_000013_0,attempt_1672901779104_0001_m_000014_0,attempt_1672901779104_0001_m_000015_0,attempt_1672901779104_0001_m_000019_0,attempt_1672901779104_0001_m_000018_0,attempt_1672901779104_0001_m_000016_0,attempt_1672901779104_0001_m_000017_0,attempt_1672901779104_0001_m_000020_0,attempt_1672901779104_0001_m_000023_0 HTTP/1.1
 *
 * Response Headers
 * ===================
 * HTTP/1.1 200 OK
 * Date:               Tue, 01 Mar 2011 22:44:26 GMT
 * Last-Modified:      Wed, 30 Jun 2010 21:36:48 GMT
 * Expires:            Tue, 01 Mar 2012 22:44:26 GMT
 * Cache-Control:      private, max-age=31536000
 *
 * serialized ShuffleHeader #1
 * MapOutput #1
 * serialized ShuffleHeader #2
 * MapOutput #1
 * EMPTY_LAST_CONTENT
 * </pre>
 */
public class ShuffleChannelHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(ShuffleChannelHandler.class);

  private final ShuffleChannelHandlerContext handlerCtx;

  ShuffleChannelHandler(ShuffleChannelHandlerContext ctx) {
    handlerCtx = ctx;
  }

  private List<String> splitMaps(List<String> mapq) {
    if (null == mapq) {
      return null;
    }
    final List<String> ret = new ArrayList<>();
    for (String s : mapq) {
      Collections.addAll(ret, s.split(","));
    }
    return ret;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx)
      throws Exception {
    LOG.debug("Executing channelActive, channel id: {}", ctx.channel().id());
    int numConnections = handlerCtx.activeConnections.incrementAndGet();
    if ((handlerCtx.maxShuffleConnections > 0) && (numConnections > handlerCtx.maxShuffleConnections)) {
      LOG.info(String.format("Current number of shuffle connections (%d) is " +
              "greater than the max allowed shuffle connections (%d)",
          handlerCtx.allChannels.size(), handlerCtx.maxShuffleConnections));

      Map<String, String> headers = new HashMap<>(1);
      // notify fetchers to backoff for a while before closing the connection
      // if the shuffle connection limit is hit. Fetchers are expected to
      // handle this notification gracefully, that is, not treating this as a
      // fetch failure.
      headers.put(RETRY_AFTER_HEADER, String.valueOf(FETCH_RETRY_DELAY));
      sendError(ctx, "", TOO_MANY_REQ_STATUS, headers);
    } else {
      super.channelActive(ctx);
      handlerCtx.allChannels.add(ctx.channel());
      LOG.debug("Added channel: {}, channel id: {}. Accepted number of connections={}",
          ctx.channel(), ctx.channel().id(), handlerCtx.activeConnections.get());
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOG.debug("Executing channelInactive, channel id: {}", ctx.channel().id());
    super.channelInactive(ctx);
    int noOfConnections = handlerCtx.activeConnections.decrementAndGet();
    LOG.debug("New value of Accepted number of connections={}", noOfConnections);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
    Channel channel = ctx.channel();
    LOG.debug("Received HTTP request: {}, channel id: {}", request, channel.id());
    if (request.method() != GET) {
      sendError(ctx, METHOD_NOT_ALLOWED);
      return;
    }
    // Check whether the shuffle version is compatible
    String shuffleVersion = ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION;
    String httpHeaderName = ShuffleHeader.DEFAULT_HTTP_HEADER_NAME;
    if (request.headers() != null) {
      shuffleVersion = request.headers().get(ShuffleHeader.HTTP_HEADER_VERSION);
      httpHeaderName = request.headers().get(ShuffleHeader.HTTP_HEADER_NAME);
      LOG.debug("Received from request header: ShuffleVersion={} header name={}, channel id: {}",
          shuffleVersion, httpHeaderName, channel.id());
    }
    if (request.headers() == null ||
        !ShuffleHeader.DEFAULT_HTTP_HEADER_NAME.equals(httpHeaderName) ||
        !ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION.equals(shuffleVersion)) {
      sendError(ctx, "Incompatible shuffle request version", BAD_REQUEST);
    }
    final Map<String, List<String>> q =
        new QueryStringDecoder(request.uri()).parameters();
    final List<String> keepAliveList = q.get("keepAlive");
    boolean keepAliveParam = false;
    if (keepAliveList != null && keepAliveList.size() == 1) {
      keepAliveParam = Boolean.parseBoolean(keepAliveList.get(0));
      if (LOG.isDebugEnabled()) {
        LOG.debug("KeepAliveParam: {} : {}, channel id: {}",
            keepAliveList, keepAliveParam, channel.id());
      }
    }
    final List<String> mapIds = splitMaps(q.get("map"));
    final List<String> reduceQ = q.get("reduce");
    final List<String> jobQ = q.get("job");
    if (LOG.isDebugEnabled()) {
      LOG.debug("RECV: " + request.uri() +
          "\n  mapId: " + mapIds +
          "\n  reduceId: " + reduceQ +
          "\n  jobId: " + jobQ +
          "\n  keepAlive: " + keepAliveParam +
          "\n  channel id: " + channel.id());
    }

    if (mapIds == null || reduceQ == null || jobQ == null) {
      sendError(ctx, "Required param job, map and reduce", BAD_REQUEST);
      return;
    }
    if (reduceQ.size() != 1 || jobQ.size() != 1) {
      sendError(ctx, "Too many job/reduce parameters", BAD_REQUEST);
      return;
    }

    int reduceId;
    String jobId;
    try {
      reduceId = Integer.parseInt(reduceQ.get(0));
      jobId = jobQ.get(0);
    } catch (NumberFormatException e) {
      sendError(ctx, "Bad reduce parameter", BAD_REQUEST);
      return;
    } catch (IllegalArgumentException e) {
      sendError(ctx, "Bad job parameter", BAD_REQUEST);
      return;
    }
    final String reqUri = request.uri();
    if (null == reqUri) {
      // TODO? add upstream?
      sendError(ctx, FORBIDDEN);
      return;
    }
    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
    try {
      verifyRequest(jobId, ctx, request, response,
          new URL("http", "", handlerCtx.getPort(), reqUri));
    } catch (IOException e) {
      LOG.warn("Shuffle failure ", e);
      sendError(ctx, e.getMessage(), UNAUTHORIZED);
      return;
    }

    Map<String, MapOutputInfo> mapOutputInfoMap = new HashMap<>();
    ChannelPipeline pipeline = channel.pipeline();
    ShuffleHandler.TimeoutHandler timeoutHandler = (ShuffleHandler.TimeoutHandler)pipeline.get(TIMEOUT_HANDLER);
    timeoutHandler.setEnabledTimeout(false);
    String user = handlerCtx.userRsrc.get(jobId);

    try {
      populateHeaders(mapIds, jobId, user, reduceId,
          response, keepAliveParam, mapOutputInfoMap);
    } catch(IOException e) {
      channel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
      LOG.error("Shuffle error while populating headers. Channel id: " + channel.id(), e);
      sendError(ctx, getErrorMessage(e), INTERNAL_SERVER_ERROR);
      return;
    }
    channel.write(response);

    //Initialize one ReduceContext object per channelRead call
    boolean keepAlive = keepAliveParam || handlerCtx.connectionKeepAliveEnabled;
    ReduceContext reduceContext = new ReduceContext(mapIds, reduceId, ctx,
        user, mapOutputInfoMap, jobId, keepAlive);

    sendMap(reduceContext);
  }

  /**
   * Calls sendMapOutput for the mapId pointed by ReduceContext.mapsToSend
   * and increments it. This method is first called by messageReceived()
   * maxSessionOpenFiles times and then on the completion of every
   * sendMapOutput operation. This limits the number of open files on a node,
   * which can get really large(exhausting file descriptors on the NM) if all
   * sendMapOutputs are called in one go, as was done previous to this change.
   * @param reduceContext used to call sendMapOutput with correct params.
   */
  public void sendMap(ReduceContext reduceContext) {
    LOG.trace("Executing sendMap");
    if (reduceContext.getMapsToSend().get() <
        reduceContext.getMapIds().size()) {
      int nextIndex = reduceContext.getMapsToSend().getAndIncrement();
      String mapId = reduceContext.getMapIds().get(nextIndex);

      try {
        MapOutputInfo info = reduceContext.getInfoMap().get(mapId);
        if (info == null) {
          info = getMapOutputInfo(mapId, reduceContext.getReduceId(),
              reduceContext.getJobId(), reduceContext.getUser());
        }
        LOG.trace("Calling sendMapOutput");
        ChannelFuture nextMap = sendMapOutput(
            reduceContext.getCtx(),
            reduceContext.getCtx().channel(),
            reduceContext.getUser(), mapId,
            reduceContext.getReduceId(), info);
        if (nextMap == null) {
          //This can only happen if spill file was not found
          sendError(reduceContext.getCtx(), NOT_FOUND);
          LOG.trace("Returning nextMap: null");
          return;
        }
        nextMap.addListener(new ReduceMapFileCount(this, reduceContext));
      } catch (IOException e) {
        if (e instanceof DiskChecker.DiskErrorException) {
          LOG.error("Shuffle error: " + e);
        } else {
          LOG.error("Shuffle error: ", e);
        }
        String errorMessage = getErrorMessage(e);
        sendError(reduceContext.getCtx(), errorMessage,
            INTERNAL_SERVER_ERROR);
      }
    }
  }

  private String getErrorMessage(Throwable t) {
    StringBuilder sb = new StringBuilder(t.getMessage());
    while (t.getCause() != null) {
      sb.append(t.getCause().getMessage());
      t = t.getCause();
    }
    return sb.toString();
  }

  protected MapOutputInfo getMapOutputInfo(String mapId, int reduce, String jobId, String user) throws IOException {
    ShuffleHandler.AttemptPathInfo pathInfo;
    try {
      ShuffleHandler.AttemptPathIdentifier identifier = new ShuffleHandler.AttemptPathIdentifier(
          jobId, user, mapId);
      pathInfo = handlerCtx.pathCache.get(identifier);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Retrieved pathInfo for " + identifier +
            " check for corresponding loaded messages to determine whether" +
            " it was loaded or cached");
      }
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new RuntimeException(e.getCause());
      }
    }

    IndexRecord info = handlerCtx.indexCache.getIndexInformation(mapId, reduce, pathInfo.indexPath, user);

    if (LOG.isDebugEnabled()) {
      LOG.debug("getMapOutputInfo: jobId=" + jobId + ", mapId=" + mapId +
          ",dataFile=" + pathInfo.dataPath + ", indexFile=" +
          pathInfo.indexPath);
    }

    return new MapOutputInfo(pathInfo.dataPath, info);
  }

  protected void populateHeaders(List<String> mapIds, String jobId,
                                 String user, int reduce, HttpResponse response,
                                 boolean keepAliveParam, Map<String, MapOutputInfo> mapOutputInfoMap)
      throws IOException {

    long contentLength = 0;
    for (String mapId : mapIds) {
      MapOutputInfo outputInfo = getMapOutputInfo(mapId, reduce, jobId, user);
      if (mapOutputInfoMap.size() < handlerCtx.mapOutputMetaInfoCacheSize) {
        mapOutputInfoMap.put(mapId, outputInfo);
      }

      ShuffleHeader header =
          new ShuffleHeader(mapId, outputInfo.indexRecord.partLength,
              outputInfo.indexRecord.rawLength, reduce);
      DataOutputBuffer dob = new DataOutputBuffer();
      header.write(dob);
      contentLength += outputInfo.indexRecord.partLength;
      contentLength += dob.getLength();
    }

    // Now set the response headers.
    setResponseHeaders(response, keepAliveParam, contentLength);

    // this audit log is disabled by default,
    // to turn it on please enable this audit log
    // on log4j.properties by uncommenting the setting
    if (AUDITLOG.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder("shuffle for ");
      sb.append(jobId).append(" reducer ").append(reduce);
      sb.append(" length ").append(contentLength);
      if (AUDITLOG.isTraceEnabled()) {
        // For trace level logging, append the list of mappers
        sb.append(" mappers: ").append(mapIds);
        AUDITLOG.trace(sb.toString());
      } else {
        AUDITLOG.debug(sb.toString());
      }
    }
  }

  protected void setResponseHeaders(HttpResponse response,
                                    boolean keepAliveParam, long contentLength) {
    if (!handlerCtx.connectionKeepAliveEnabled && !keepAliveParam) {
      response.headers().set(HttpHeader.CONNECTION.asString(), CONNECTION_CLOSE);
    } else {
      response.headers().set(HttpHeader.CONNECTION.asString(),
          HttpHeader.KEEP_ALIVE.asString());
      response.headers().set(HttpHeader.KEEP_ALIVE.asString(),
          "timeout=" + handlerCtx.connectionKeepAliveTimeOut);
    }

    response.headers().set(HttpHeader.CONTENT_LENGTH.asString(),
        String.valueOf(contentLength));
    response.headers().set(TRANSFER_ENCODING, CHUNKED);

    LOG.info("Content Length in shuffle : " + contentLength);
  }

  static class MapOutputInfo {
    final Path mapOutputFileName;
    final IndexRecord indexRecord;

    MapOutputInfo(Path mapOutputFileName, IndexRecord indexRecord) {
      this.mapOutputFileName = mapOutputFileName;
      this.indexRecord = indexRecord;
    }
  }

  protected void verifyRequest(String appid, ChannelHandlerContext ctx,
                               HttpRequest request, HttpResponse response, URL requestUri)
      throws IOException {
    SecretKey tokenSecret = handlerCtx.secretManager.retrieveTokenSecret(appid);
    if (null == tokenSecret) {
      LOG.info("Request for unknown token {}, channel id: {}", appid, ctx.channel().id());
      throw new IOException("Could not find jobid");
    }
    // encrypting URL
    String encryptedURL = SecureShuffleUtils.buildMsgFrom(requestUri);
    // hash from the fetcher
    String urlHashStr =
        request.headers().get(SecureShuffleUtils.HTTP_HEADER_URL_HASH);
    if (urlHashStr == null) {
      LOG.info("Missing header hash for {}, channel id: {}", appid, ctx.channel().id());
      throw new IOException("fetcher cannot be authenticated");
    }
    if (LOG.isDebugEnabled()) {
      int len = urlHashStr.length();
      LOG.debug("Verifying request. encryptedURL:{}, hash:{}, channel id: " +
              "{}", encryptedURL,
          urlHashStr.substring(len - len / 2, len - 1), ctx.channel().id());
    }
    // verify - throws exception
    SecureShuffleUtils.verifyReply(urlHashStr, encryptedURL, tokenSecret);
    // verification passed - encode the reply
    String reply = SecureShuffleUtils.generateHash(urlHashStr.getBytes(Charsets.UTF_8),
        tokenSecret);
    response.headers().set(
        SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH, reply);
    // Put shuffle version into http header
    response.headers().set(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    response.headers().set(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    if (LOG.isDebugEnabled()) {
      int len = reply.length();
      LOG.debug("Fetcher request verified. " +
              "encryptedURL: {}, reply: {}, channel id: {}",
          encryptedURL, reply.substring(len - len / 2, len - 1),
          ctx.channel().id());
    }
  }

  protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx, Channel ch,
                                        String user, String mapId, int reduce, MapOutputInfo mapOutputInfo)
      throws IOException {
    final IndexRecord info = mapOutputInfo.indexRecord;
    final ShuffleHeader header = new ShuffleHeader(mapId, info.partLength, info.rawLength,
        reduce);
    final DataOutputBuffer dob = new DataOutputBuffer();
    header.write(dob);
    ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
    final File spillfile =
        new File(mapOutputInfo.mapOutputFileName.toString());
    RandomAccessFile spill;
    try {
      spill = SecureIOUtils.openForRandomRead(spillfile, "r", user, null);
    } catch (FileNotFoundException e) {
      LOG.info("{} not found. Channel id: {}", spillfile, ctx.channel().id());
      return null;
    }
    ChannelFuture writeFuture;
    if (ch.pipeline().get(SslHandler.class) == null) {
      // TODO this is just for easy reproduction, the fileregion should be used here
      LOG.error("tomi test");
      final FadvisedChunkedFile chunk = new FadvisedChunkedFile(spill,
          info.startOffset, info.partLength, handlerCtx.sslFileBufferSize,
          handlerCtx.manageOsCache, handlerCtx.readaheadLength, handlerCtx.readaheadPool,
          spillfile.getAbsolutePath());
      writeFuture = ch.writeAndFlush(chunk);

      /*
      final FadvisedFileRegion partition = new FadvisedFileRegion(spill,
          info.startOffset, info.partLength, handlerCtx.manageOsCache, handlerCtx.readaheadLength,
          handlerCtx.readaheadPool, spillfile.getAbsolutePath(),
          handlerCtx.shuffleBufferSize, handlerCtx.shuffleTransferToAllowed);
      writeFuture = ch.writeAndFlush(partition);
      writeFuture.addListener(new ChannelFutureListener() {
        // TODO error handling; distinguish IO/connection failures,
        //      attribute to appropriate spill output
        @Override
        public void operationComplete(ChannelFuture future) {
          if (future.isSuccess()) {
            partition.transferSuccessful();
          }
          partition.deallocate();
        }
      });
      */
    } else {
      // HTTPS cannot be done with zero copy.
      final FadvisedChunkedFile chunk = new FadvisedChunkedFile(spill,
          info.startOffset, info.partLength, handlerCtx.sslFileBufferSize,
          handlerCtx.manageOsCache, handlerCtx.readaheadLength, handlerCtx.readaheadPool,
          spillfile.getAbsolutePath());
      writeFuture = ch.writeAndFlush(chunk);
    }
    handlerCtx.metrics.shuffleConnections.incr();
    handlerCtx.metrics.shuffleOutputBytes.incr(info.partLength); // optimistic
    return writeFuture;
  }

  protected void sendError(ChannelHandlerContext ctx,
                           HttpResponseStatus status) {
    sendError(ctx, "", status);
  }

  protected void sendError(ChannelHandlerContext ctx, String message,
                           HttpResponseStatus status) {
    sendError(ctx, message, status, Collections.emptyMap());
  }

  protected void sendError(ChannelHandlerContext ctx, String msg,
                           HttpResponseStatus status, Map<String, String> headers) {
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status,
        Unpooled.copiedBuffer(msg, CharsetUtil.UTF_8));
    response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
    // Put shuffle version into http header
    response.headers().set(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    response.headers().set(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    for (Map.Entry<String, String> header : headers.entrySet()) {
      response.headers().set(header.getKey(), header.getValue());
    }

    // Close the connection as soon as the error message is sent.
    ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    Channel ch = ctx.channel();
    if (cause instanceof TooLongFrameException) {
      LOG.trace("TooLongFrameException, channel id: {}", ch.id());
      sendError(ctx, BAD_REQUEST);
      return;
    } else if (cause instanceof IOException) {
      if (cause instanceof ClosedChannelException) {
        LOG.debug("Ignoring closed channel error, channel id: " + ch.id(), cause);
        return;
      }
      String message = String.valueOf(cause.getMessage());
      if (IGNORABLE_ERROR_MESSAGE.matcher(message).matches()) {
        LOG.debug("Ignoring client socket close, channel id: " + ch.id(), cause);
        return;
      }
    }

    LOG.error("Shuffle error. Channel id: " + ch.id(), cause);
    if (ch.isActive()) {
      sendError(ctx, INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Maintain parameters per messageReceived() Netty context.
   * Allows sendMapOutput calls from operationComplete()
   */
  public static class ReduceContext {
    private final List<String> mapIds;
    private final AtomicInteger mapsToWait;
    private final AtomicInteger mapsToSend;
    private final int reduceId;
    private final ChannelHandlerContext ctx;
    private final String user;
    private final Map<String, ShuffleChannelHandler.MapOutputInfo> infoMap;
    private final String jobId;
    private final boolean keepAlive;

    ReduceContext(List<String> mapIds, int rId,
                  ChannelHandlerContext context, String usr,
                  Map<String, ShuffleChannelHandler.MapOutputInfo> mapOutputInfoMap,
                  String jobId, boolean keepAlive) {

      this.mapIds = mapIds;
      this.reduceId = rId;
      /*
       * Atomic count for tracking the no. of map outputs that are yet to
       * complete. Multiple futureListeners' operationComplete() can decrement
       * this value asynchronously. It is used to decide when the channel should
       * be closed.
       */
      this.mapsToWait = new AtomicInteger(mapIds.size());
      /*
       * Atomic count for tracking the no. of map outputs that have been sent.
       * Multiple sendMap() calls can increment this value
       * asynchronously. Used to decide which mapId should be sent next.
       */
      this.mapsToSend = new AtomicInteger(0);
      this.ctx = context;
      this.user = usr;
      this.infoMap = mapOutputInfoMap;
      this.jobId = jobId;
      this.keepAlive = keepAlive;
    }

    public int getReduceId() {
      return reduceId;
    }

    public ChannelHandlerContext getCtx() {
      return ctx;
    }

    public String getUser() {
      return user;
    }

    public Map<String, ShuffleChannelHandler.MapOutputInfo> getInfoMap() {
      return infoMap;
    }

    public String getJobId() {
      return jobId;
    }

    public List<String> getMapIds() {
      return mapIds;
    }

    public AtomicInteger getMapsToSend() {
      return mapsToSend;
    }

    public AtomicInteger getMapsToWait() {
      return mapsToWait;
    }

    public boolean getKeepAlive() {
      return keepAlive;
    }
  }

  static class ReduceMapFileCount implements ChannelFutureListener {
    private final ShuffleChannelHandler handler;
    private final ReduceContext reduceContext;

    ReduceMapFileCount(ShuffleChannelHandler handler, ReduceContext rc) {
      this.handler = handler;
      this.reduceContext = rc;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      LOG.trace("operationComplete");
      if (!future.isSuccess()) {
        LOG.error("Future is unsuccessful. Cause: ", future.cause());
        future.channel().close();
        return;
      }
      int waitCount = this.reduceContext.getMapsToWait().decrementAndGet();
      if (waitCount == 0) {
        LOG.trace("Finished with all map outputs");
        ChannelFuture lastContentFuture = future.channel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        handler.handlerCtx.metrics.operationComplete(future);

        // Let the idle timer handler close keep-alive connections
        if (reduceContext.getKeepAlive()) {
          ChannelPipeline pipeline = future.channel().pipeline();
          ShuffleHandler.TimeoutHandler timeoutHandler =
              (ShuffleHandler.TimeoutHandler)pipeline.get(TIMEOUT_HANDLER);
          timeoutHandler.setEnabledTimeout(true);
        } else {
          lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
      } else {
        LOG.trace("operationComplete, waitCount > 0, invoking sendMap with reduceContext");
        handler.sendMap(reduceContext);
      }
    }
  }
}
