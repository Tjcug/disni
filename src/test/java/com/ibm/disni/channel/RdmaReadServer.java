package com.ibm.disni.channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * locate org.apache.storm.messaging.rdma
 * Created by mastertj on 2018/8/27.
 * java -cp disni-1.6-jar-with-dependencies.jar:disni-1.6-tests.jar com.ibm.disni.channel.RdmaReadServer
 */
public class RdmaReadServer {
    private static final Logger logger = LoggerFactory.getLogger(RdmaReadServer.class);

    public static void main(String[] args) throws Exception {
        RdmaNode rdmaServer=new RdmaNode("10.10.0.25", false, new RdmaShuffleConf(), new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf) {
                logger.info("success");
            }

            @Override
            public void onFailure(Throwable exception) {
                exception.printStackTrace();
            }
        });

        RdmaBufferManager rdmaBufferManager = rdmaServer.getRdmaBufferManager();
        RdmaRegisteredBuffer registeredDataBuffer=new RdmaRegisteredBuffer(rdmaBufferManager,100);
        RdmaRegisteredBuffer sendAddressBuffer=new RdmaRegisteredBuffer(rdmaBufferManager,100);

        ByteBuffer dataBuf = registeredDataBuffer.getByteBuffer(100);
        dataBuf.asCharBuffer().put("This is a RDMA/read on stag " + sendAddressBuffer.getLkey() + " !");
        dataBuf.clear();

        ByteBuffer sendBuf = sendAddressBuffer.getByteBuffer(100);
        sendBuf.putLong(registeredDataBuffer.getRegisteredAddress());
        sendBuf.putInt(registeredDataBuffer.getLkey());
        sendBuf.putInt(100);
        sendBuf.clear();

        RdmaChannel rdmaChannel = rdmaServer.getRdmaChannel(new InetSocketAddress("10.10.0.24", 1955), true);
        rdmaChannel.rdmaSendInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf) {

            }

            @Override
            public void onFailure(Throwable exception) {

            }
        },new long[]{sendAddressBuffer.getRegisteredAddress()},new int[]{sendAddressBuffer.getLkey()},new int[]{100});

        Thread.sleep(10000000);
    }
}
