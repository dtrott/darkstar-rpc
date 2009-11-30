package com.projectdarkstar.rpc.server;

import com.example.Example.SimpleService;
import com.example.Example.StringPair;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import static com.projectdarkstar.rpc.server.TestUtils.buildPair;
import com.projectdarkstar.rpc.common.DarkstarRpc;
import com.sun.sgs.app.AppContext;
import com.sun.sgs.app.Channel;
import com.sun.sgs.app.ChannelListener;
import com.sun.sgs.app.ClientSession;
import com.sun.sgs.app.Delivery;
import net.java.dev.mocksgs.MockChannel;
import net.java.dev.mocksgs.MockChannelFactory;
import net.java.dev.mocksgs.MockChannelManager;
import net.java.dev.mocksgs.MockSGS;
import org.easymock.Capture;
import org.easymock.classextension.EasyMock;
import static org.easymock.classextension.EasyMock.capture;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.eq;
import static org.easymock.classextension.EasyMock.expect;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

@Test
public class TestRpcCall {
    @SuppressWarnings("unchecked")
     static <T> Class<T> suppressGenerics(Class in) {
         return in;
     }

    private final static Class<RpcCallback<StringPair>> stringCallbackClass = suppressGenerics(RpcCallback.class);


    @Test
    public void blah() throws Exception {
        final MockChannelFactory mockChannelFactory = createMock("mockChannelFactory", MockChannelFactory.class);
        final MockChannel mockChannel = createMock("mockChannel", MockChannel.class);

        final RpcCallback<StringPair> mockCallback = EasyMock.createMock("mockCallback", stringCallbackClass);

        Capture<ByteBuffer> message1 = new Capture<ByteBuffer>();
        Capture<ByteBuffer> message2 = new Capture<ByteBuffer>();

        expect(mockChannelFactory.createChannel(
            eq("testChannel"), EasyMock.<ChannelListener>anyObject(), eq(Delivery.RELIABLE))).andReturn(mockChannel);

        expect(mockChannel.send(EasyMock.<ClientSession>isNull(), capture(message1))).andReturn(mockChannel);
        expect(mockChannel.send(EasyMock.<ClientSession>isNull(), capture(message2))).andReturn(mockChannel);

        // Init everything
        replay(mockChannelFactory, mockChannel, mockCallback);
        MockSGS.init();
        ServerNamingServiceFactory.init();

        // Install the mocks
        final MockChannelManager manager = (MockChannelManager) AppContext.getChannelManager();
        manager.setMockChannelFactor(mockChannelFactory);

        // Start the test.


        final ServerChannelRpcListener listener = new ServerChannelRpcListener(ServerNamingServiceFactory.getNamingService());
        final Channel channel = AppContext.getChannelManager().createChannel("testChannel", listener, Delivery.RELIABLE);
        listener.setChannel(channel);
        final DarkstarRpc darkstarRpc = listener.getDarkstarRpc();


        RpcChannel rpcChannel =darkstarRpc.getRpcChannel();
        final RpcController controller = darkstarRpc.newRpcController();

        final SimpleService.Stub helloStub = SimpleService.newStub(rpcChannel);

        helloStub.exchange(controller, buildPair("hello", "world"), mockCallback);

        final ByteBuffer buffer = message1.getValue();
        assertEquals(buffer.get(), 1);
        assertEquals(buffer.get(), 0);
        assertEquals(buffer.getLong(), 1);
        assertEquals(buffer.remaining(), 14);

        final StringPair pair = buildPair(buffer);

        assertEquals(pair.getName(), "hello");
        assertEquals(pair.getValue(), "world");

        buffer.flip();

        darkstarRpc.registerService(SimpleService.Interface.class, new SimpleServiceImpl());

        listener.receivedMessage(channel, null, buffer);

        final ByteBuffer value = message2.getValue();
        assertEquals(value.get(), -127);
        assertEquals(value.getLong(), 1);
        assertEquals(value.remaining(), 10);

        final StringPair message = StringPair.newBuilder().mergeFrom(value.array(), value.position(), value.remaining()).build();
        assertEquals(message.getName(), "Life");

        verify(mockChannelFactory, mockChannel, mockCallback);
    }
}

