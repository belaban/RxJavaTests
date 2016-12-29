package org.test;


import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.reactivex.netty.protocol.tcp.server.TcpServer;
import org.jgroups.util.Util;
import rx.Observable;

import static io.netty.util.CharsetUtil.UTF_8;

/**
 * rxnetty based server
 * @author Bela Ban
 * @since x.y
 */
public class Server {
    protected TcpServer<String,String> srv;
    protected static final double CONVERSION=1.06448;

    protected void start(int port) {
        srv=TcpServer.newServer(port)
          .pipelineConfigurator(pipeline -> {
              pipeline.addLast(new LineBasedFrameDecoder(1024));
              pipeline.addLast(new StringDecoder(UTF_8));
          });

        srv.start(conn -> {
            Observable<String> output=conn.getInput()
              .doOnNext(el -> log(String.format("el is %s\n", el)))
              .map(eur_str -> {
                  double eur=Double.valueOf(eur_str);
                  double usd=convert(eur);
                  return String.format("%.2f EUR are %.2f USD\n", eur, usd);
              });
            return conn.writeAndFlushOnEach(output);
        }).awaitShutdown();
    }

    protected static void log(String msg) {
        System.out.printf("[%s] %s", Thread.currentThread().getName(), msg);
    }

    protected static double convert(double eur) {
        Util.sleep(1000);
        return eur * CONVERSION;
    }

    public static void main(String[] args) {
        new Server().start(args.length > 0? Integer.valueOf(args[0]) : 8080);
    }
}
