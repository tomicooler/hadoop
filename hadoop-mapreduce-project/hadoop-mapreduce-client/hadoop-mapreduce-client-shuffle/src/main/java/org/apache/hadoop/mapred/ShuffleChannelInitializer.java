package org.apache.hadoop.mapred;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.apache.hadoop.security.ssl.SSLFactory;

import static org.apache.hadoop.mapred.ShuffleHandler.TIMEOUT_HANDLER;

public class ShuffleChannelInitializer extends ChannelInitializer<SocketChannel> {

  private static final int MAX_CONTENT_LENGTH = 1 << 16;

  private final ShuffleChannelHandlerContext handlerContext;
  private final SSLFactory sslFactory;


  public ShuffleChannelInitializer(ShuffleChannelHandlerContext ctx, SSLFactory sslFactory) {
    this.handlerContext = ctx;
    this.sslFactory = sslFactory;
  }

  @Override
  public void initChannel(SocketChannel ch) throws GeneralSecurityException, IOException {
    ChannelPipeline pipeline = ch.pipeline();
    if (sslFactory != null) {
      pipeline.addLast("ssl", new SslHandler(sslFactory.createSSLEngine()));
    }
    pipeline.addLast("http", new HttpServerCodec());
    pipeline.addLast("aggregator", new HttpObjectAggregator(MAX_CONTENT_LENGTH));
    pipeline.addLast("chunking", new ChunkedWriteHandler());
    pipeline.addLast("shuffle", new ShuffleChannelHandler(handlerContext));
    pipeline.addLast(TIMEOUT_HANDLER, new ShuffleHandler.TimeoutHandler(handlerContext.connectionKeepAliveTimeOut));
    // TODO factor security manager into pipeline
    // TODO factor out encode/decode to permit binary shuffle
    // TODO factor out decode of index to permit alt. models
  }
}
