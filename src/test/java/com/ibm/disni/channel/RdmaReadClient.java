package com.ibm.disni.channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * locate org.apache.storm.messaging.rdma
 * Created by mastertj on 2018/8/27.
 * java -cp disni-1.6-jar-with-dependencies.jar:disni-1.6-tests.jar com.ibm.disni.channel.RdmaReadClient
 */
public class RdmaReadClient {
    private static final Logger logger = LoggerFactory.getLogger(RdmaReadClient.class);

    public static void main(String[] args) throws Exception {
        RdmaNode rdmaClient=new RdmaNode("10.10.0.24", true, new RdmaShuffleConf(), new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf) {
                logger.info("success");
            }

            @Override
            public void onFailure(Throwable exception) {
                exception.printStackTrace();
            }
        });

        RdmaBufferManager rdmaBufferManager = rdmaClient.getRdmaBufferManager();
        RdmaRegisteredBuffer registeredBuffer=new RdmaRegisteredBuffer(rdmaBufferManager,100);
        ByteBuffer byteBuffer = registeredBuffer.getByteBuffer(100);

        RdmaChannel rdmaChannel = rdmaClient.getRdmaChannel(new InetSocketAddress("10.10.0.25", 1955), true);

        long addr = byteBuffer.getLong();
        int lkey = byteBuffer.getInt();
        int length = byteBuffer.getInt();
        logger.info("second add: "+addr+" lkey: "+lkey+" length: "+length);

        rdmaChannel.rdmaReadInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf) {
                logger.info("RdmaActiveReadClient::read memory from server: " + buf.asCharBuffer().toString());
            }
            @Override
            public void onFailure(Throwable exception) {

            }
        },registeredBuffer.getRegisteredAddress(),registeredBuffer.getLkey(),new int[]{length},new long[]{addr},new int[]{lkey});

        logger.info("RdmaActiveReadClient::read memory from server: " + byteBuffer.asCharBuffer().toString());
        Thread.sleep(10000000);
    }
}
