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
        RdmaNode rdmaServer=new RdmaNode("10.10.0.25", true, new RdmaShuffleConf(), new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf) {
                logger.info("success1111");
            }

            @Override
            public void onFailure(Throwable exception) {
                exception.printStackTrace();
            }
        });

        InetSocketAddress address = null;
        RdmaChannel rdmaChannel=null;

        while (true){
            address = rdmaServer.passiveRdmaInetSocketMap.get("10.10.0.24");
            if(address!=null){
                rdmaChannel=rdmaServer.passiveRdmaChannelMap.get(address);
                if(rdmaChannel.isConnected())
                    break;
            }
        }

        logger.info(rdmaServer.passiveRdmaInetSocketMap.toString());

        VerbsTools commRdma = rdmaChannel.getCommRdma();

        RdmaBuffer sendMr = rdmaChannel.getSendBuffer();
        ByteBuffer sendBuf = sendMr.getByteBuffer();
        RdmaBuffer dataMr = rdmaChannel.getDataBuffer();
        ByteBuffer dataBuf = dataMr.getByteBuffer();
        RdmaBuffer recvMr = rdmaChannel.getReceiveBuffer();
        ByteBuffer recvBuf = recvMr.getByteBuffer();

        ByteBuffer byteBuffer= ByteBuffer.allocateDirect(1024);
        byteBuffer.asCharBuffer().put("This is a RDMA/read on stag " + dataMr.getLkey() + " !");
        byteBuffer.clear();

        dataBuf.asCharBuffer().put("This is a RDMA/read on stag " + dataMr.getLkey() + " !");
        dataBuf.clear();

        sendBuf.putLong(dataMr.getAddress());
        sendBuf.putInt(dataMr.getLkey());
        sendBuf.putInt(dataMr.getLength());
        sendBuf.clear();

        logger.info("first add: "+dataMr.getAddress()+" lkey: "+dataMr.getLkey()+" length: "+dataMr.getLength());
        logger.info("dataBuf: "+ dataBuf.asCharBuffer().toString());
        logger.info("byteBuffer: "+ byteBuffer.asCharBuffer().toString());

        //post a send call, here we send a message which include the RDMA information of a data buffer
        recvBuf.clear();
        dataBuf.clear();
        sendBuf.clear();
        rdmaChannel.rdmaSendInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf) {
                logger.info("RDMA SEND Address Success");
            }

            @Override
            public void onFailure(Throwable exception) {
                exception.printStackTrace();
            }
        },new long[]{sendMr.getAddress()},new int[]{sendMr.getLkey()},new int[]{sendMr.getLength()});

        logger.info("RDMA SEND Address Success");

        System.out.println("VerbsServer::stag info sent");

        //wait for the final message from the server
        rdmaChannel.completeSGRecv();

        System.out.println("VerbsServer::done");
    }
}
