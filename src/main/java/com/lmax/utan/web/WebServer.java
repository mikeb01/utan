package com.lmax.utan.web;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;

public class WebServer implements AutoCloseable
{
    private final int port;
    private final NioEventLoopGroup bossGroup;
    private final NioEventLoopGroup workerGroup;
    private final Channel ch;

    public WebServer(int port) throws InterruptedException
    {
        this.port = port;
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap
            .option(ChannelOption.SO_BACKLOG, 1024)
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new ServerInitializer(null));

        ch = bootstrap.bind(port).sync().channel();
    }

    public void sync() throws InterruptedException
    {
        System.err.printf("Open your web browser and navigate to http://127.0.0.1:%d/%n", port);
        ch.closeFuture().sync();
    }

    @Override
    public void close() throws Exception
    {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    private static class ServerInitializer extends ChannelInitializer<SocketChannel>
    {
        private final SslContext sslCtx;

        public ServerInitializer(SslContext sslCtx)
        {
            this.sslCtx = sslCtx;
        }

        @Override
        public void initChannel(SocketChannel ch)
        {
            ChannelPipeline p = ch.pipeline();
            if (sslCtx != null)
            {
                p.addLast(sslCtx.newHandler(ch.alloc()));
            }
            p.addLast(new HttpServerCodec());
            p.addLast(new HttpHelloWorldServerHandler());
        }
    }
}
