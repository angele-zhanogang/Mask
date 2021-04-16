package org.jboss.netty.example.discard;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;

public class TimeServerHandler extends SimpleChannelHandler {

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        //super.channelConnected(ctx, e);
        Channel ch = e.getChannel();
        ChannelBuffer time = ChannelBuffers.buffer(4);
        int num = (int) (System.currentTimeMillis() / 1000L + 2208988800L);
        time.writeInt(num);

        ChannelFuture f = ch.write(time);
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                Channel ch = future.getChannel();
                ch.close();
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}
