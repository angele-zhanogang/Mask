### Netty

通过netty实现简单通信

程序依赖：

```xml
<dependency>
     <groupId>io.netty</groupId>
     <artifactId>netty</artifactId>
     <version>3.10.5.Final</version>
</dependency>
```

### 1.Netty简单处理

#### 1.1 实现事件处理`DiscardServerHandler`

​	继承 `SimpleChannelHandler`，目的是为了是实现`ChannelHandler`。`SimpleChannelHandle`r提供了可以覆盖的各种事件处理程序方法。目前继承`SimpleChannelHandler`就足够使用，而不需要自己实现处理接口。

​	重写事件处理方法`messageReceived`,`MessageEvent`每当从客户端接收到新数据时，就调用此方法，该方法包含接受到的数据。这里从`MessageEvent`中获取数据并输出到控制台。

​	重写异常处理方法`exceptionCaught`,当netty因`I/O`处理错误而引起异常或者由于处理事件时抛出异常而由处理程序实现引发时间处理程序时，将使用调用时间处理程序方法。在大多数情况下，应该记录捕获到的异常，并在此处关闭其关联的通道，尽管此方法的实现可能会有所不同，具体取决于你要处理特殊情况时要采取的措施。例如，你想在关闭连接之前发送带有错误代码的响应信息。

```java
package org.jboss.netty.example.discard;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;

/**
 * 实现一个丢弃服务器
 * 实现一个discard协议
 * 忽略接收到的数据
 */
public class DiscardServerHandler extends SimpleChannelHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buf = (ChannelBuffer) e.getMessage();
        while (buf.readable()){
            System.out.println((char)buf.readByte());
            System.out.flush();
        }


        super.messageReceived(ctx, e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        Channel ch = e.getChannel();
        ch.close();
    }
}
```

ChannelBuffer套接字中传输的消息类型。它是一个基本的数据结构，它在Netty中存储字节序列。它与NIO相似 `ByteBuffer`，但更易于使用且更灵活。例如，Netty允许您创建一个组合`ChannelBuffer`多个`ChannelBuffers`的组合 ，从而减少了不必要的内存复制数量。

#### 1.2 构建服务`DiscardServer `

​	`ChannelFactory`是创建和管理`channel`及相关资源的工厂。他处理所有`I/O`请求并执行`I/O`生成`ChannelEvent`,`Netty`提供了各种`channelFactory`实现。在这个例子中，我们实现服务端应用程序，因此使用`NioServerSocketChannelFactory`来构建。应注意工厂本身不会创建`I/O`线程。它应该从你在构造函数中指定的线程池中获取线程，他使你可以更好的控制应如何在应用程序运行的环境中管理线程，例如带有安全管理器的应用程序服务器。

​	`ServerBootstrap`是设置服务器的帮助程序类。可以在`channel`直接使用来设置服务器。但是这个过程异常繁琐，不建议这样做。

​	配置`ChannelPipelineFactory`，每当服务器接收新的连接数时，`ChannelPipeline`都会指定的创建新的连接`ChannelPipelineFactory`。新管道包含`DiscardServerHandler`，随着应用程序变得复杂，可以向管道添加更多处理程序，并最终将此匿名类提取出来。

​	设置`channel`实现的参数。我们实现的是一个`TCP/IP`服务器，因此我们可以设置套接字选项，例如：`tcpNoDelay`和`keepAlive`。在设置中默认都添加了`child.`前缀。这表明选项将应用于`channel`而不是选项`ServerSocketChannel`。你可以执行例如 `bootstrap.setOption（“ reuseAddress”，true);` 来设置。

```java
package org.jboss.netty.example.discard;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class DiscardServer {
    public static void main(String[] args) throws Exception {
        ChannelFactory factory =
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool());

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                return Channels.pipeline(new DiscardServerHandler());
            }
        });

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        bootstrap.bind(new InetSocketAddress(8080));
    }

}
```

最后，将服务器绑定到本地的`8080`端口，可以使用`telnet localhost 8080`来测试

服务启动后，进入`telnet`后，输入字母，会在控制台显示。



### 2.编写时间服务器

#### 2.1 编写时间服务

以下例子实现的协议是TIME协议。它会发送一条包含32位整数的消息，而没有接收到任何请求，并且一旦发送了消息，便失去了连接。因为这里会忽略任何接收到的数据，而实建立连接后立即发送消息，所以我们`messageReceived`这次无法使用该 方法。相反，我们应该重写该`channelConnected`方法。以下是实现：

```java
package com.netty.time;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;

public class TimeServerHandler extends SimpleChannelHandler {
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        Channel ch = e.getChannel();
        ChannelBuffer time = ChannelBuffers.buffer(4);
        int num = (int) (System.currentTimeMillis() / 1000L + 2208988800L);
        time.writeInt(num);
        ChannelFuture future = ch.write(time);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                System.out.println("TimeServerHandler operationComplete init....");
                Channel ch = future.getChannel();
                ch.close();
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        System.out.println("TimeServerHandler exceptionCaught init....");
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}
```

#### 2.2 构建时间服务

```java
package com.netty.time;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class TimeServer {
    public static void main(String[] args) {
        ChannelFactory channelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()
        );

        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new TimeServerHandler());
            }
        });

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
		//绑定本地服务器的ip,rdate默认端口为37
        bootstrap.bind(new InetSocketAddress("192.168.72.111",37));
    }
}
```

#### 2.3 测试成果

  启动时间服务，在服务器上使用`rdate`来测试时间同步服务器

```shell
rdate -p 192.168.72.111
```

### 3 时间客户端

#### 3.1 实现事件处理

```java
package com.netty.time;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import java.util.Date;

public class TimeClientHandler extends SimpleChannelHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        long currentTimeMillis = buffer.readInt() * 1000L;
        System.out.println(new Date(currentTimeMillis));
        e.getChannel().close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}
```

#### 3.2 构建服务

```java
package com.netty.time;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class TimeClient {
    public static void main(String[] args) {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        //这里使用客户端工厂 NioClientSocketChannelFactory
        ChannelFactory factory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()
        );
        ClientBootstrap bootstrap = new ClientBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new TimeClientHandler());
            }
        });

        bootstrap.setOption("tcpNoDelay",true);
        bootstrap.setOption("keepAlive",true);

        bootstrap.bind(new InetSocketAddress(host,port));
    }
}
```

#### 3.3 测试

未知如何测试  > <



### 4 数据的传输

存在问题：在基于流的传输（如tcp/ip）中，接收到的数据存储在套接字接收缓冲区中。但是基于流的传输的缓冲区部署包的队列，而实字节队列。这意味着，即使将两条消息作为连个独立的包发送，操作系统也不会将它们视为两条消息，而是一串字节。因此，你不能保证你独到的就是你远程同伴发送的。

实际发送的数据包

```
  1 +-----+-----+-----+
  2 | ABC | DEF | GHI |
    +-----+-----+-----+
```

可能接收到的数据包

```
  1 +----+-------+---+---+
  2 | AB | CDEFG | H | I |
    +----+-------+---+---+
```

希望接收的数据包

```
  1 +-----+-----+-----+
  2 | ABC | DEF | GHI |
    +-----+-----+-----+
```



#### 4.1 使用累积缓存区

以`TimeClient`为例子

当缓冲区有4个字节时，才会解析

```java
package com.netty.time;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;

public class TImeClientBuffer extends SimpleChannelHandler {

    private final ChannelBuffer buffer = dynamicBuffer();

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buf = (ChannelBuffer) e.getMessage();
        buffer.writeBytes(buf);

        if(buffer.readableBytes() > 4){
            long millis = buffer.readInt() * 1000L;
            System.out.println(millis);
            e.getChannel().close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}
```

这样做会产生碎片，只会处理一部分数据，其他数据会丢失并且难以维护。



#### 4.2  使用FrameDecoder解决碎片问题（解码）

`FrameDecoder`是一个`ChannelHandle`,可以处理碎片问题。

每当`FrameDecoder`调用`decode`方法接收到数据时，将使用内部维护的累计缓冲区调用方法。

如果返回`null`，则表示没有足够的数据，`FrameDecoder`会在有足够数据量的时候再次调用。

如果返回的不是`null`，则表示`decode`成功的解码了消息。`FrameDecode`r将丢弃其内部累计缓冲区的读取部分（当前案例中不需要处理多条消息）。`FrameDecoder`将继续调用`decode`方法，直到返回`null`。

`FrameDecoder`和`ReplayingDecoder`允许返回任意类型的数据

```java
package com.netty.time;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class TimeDecoder extends FrameDecoder {
    @Override
    protected Object decode(ChannelHandlerContext context, Channel channel, ChannelBuffer buffer) throws Exception {
        if(buffer.readableBytes() < 4){
            return null;
        }
        
        return buffer.readBytes(4);
    }
}
```

修改服务程序，添加`TimeDecoder`

```java
bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(
                    new TimeDecoder(),
                    new TimeClientHandler()     
                );
            }
        });
```



#### 4.3 使用javabean替代ChannelBuffer

​	优点：通过分离`ChannelBuffer`从处理程序中解析信息的代码，会将程序变得更具可维护性和可重用性。

```java
package com.netty.time;

import java.util.Date;

public class UnixTime {
    private int value;

    public UnixTIme(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return new Date(value*1000L).toString();
    }
}
```

在`TimeDecoder`中使用`UnixTime`替换`channelbuffer`

```java
@Override
protected Object decode(ChannelHandlerContext context, Channel channel, ChannelBuffer buffer) throws Exception {
    if(buffer.readableBytes() < 4){
        return null;
    }
    return new UnixTime(buffer.readInt());
}
```
在`TimeClientHandler`中使用`UnixTime`替换`channelbuffer`

```java
 @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        UnixTime m = (UnixTime) e.getMessage();
        System.out.println(m);
        e.getChannel().close();
    }
```

也可以在`timeserver`的使用



#### 4.4 实现编码器

这里实现ChannelHandler的是一个转换UnixTime为ChannelBuffer的编码器。处理比解码器要简单的多，因为编码消息时不需要处理包的分段和组合。

```java
package com.netty.time;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import static org.jboss.netty.buffer.ChannelBuffers.buffer;

public class TimeEncoder extends SimpleChannelHandler {

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        UnixTIme m = (UnixTIme) e.getMessage();
        ChannelBuffer buf = buffer(4);
        buf.writeInt(m.getValue());
        Channels.write(ctx,e.getFuture(),buf);
    }
}
```

编码器会覆盖`writeRequested`方法以拦截写请求。`MessageEvent`此处的参数与指定的类型相同，

`messageReceived`但他们的解释不同。一个`ChannelEvent`可以是一个上游或者下游的事件，这取决于流动方向。例如`MessageEvent`在被调用是可以是上游事件，在被调用是可以是`messageReceived`下游事件`writeRequested`。

一旦有一个转换`pojo`到完成`ChannelBuffer`，应当转发新的缓冲区到以前`ChannelDownstreamHandler`的`ChannelPipeline`。`Channels`提供各种辅助方法来生成和发送一个`Channelevent`。在这个例子里，Channels.write(...)方法创建了一个新的消息事件并且发送给了在`ChannelPipeline`中之前`ChannelDownstreamHandler`。



#### 4.5 关闭

典型的应用关闭三个步骤：

1.关闭服务所有套接字连接

2.关闭服务所有非套接字连接

3.释放ChannelFactory占用的所有资源

```java
package com.netty.time;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;

public class Close {
    public static void main(String[] args) {
        ChannelFactory factory = ...;
        ClientBootstrap bootstrap = ...;
        ....
        ChannelFuture future = bootstrap.connect(....);
        future.awaitUninterruptibly();
        if(!future.isSuccess()){
         future.getCause().printStackTrace();   
        }
        
        future.getChannel().getCloseFuture().awaitUninterruptibly();
        factory.releaseExternalResources();
        
    }
}
```

