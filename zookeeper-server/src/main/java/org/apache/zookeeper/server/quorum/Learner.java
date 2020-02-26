/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.server.quorum;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the superclass of two of the three main actors in a ZK
 * ensemble: Followers and Observers. Both Followers and Observers share 
 * a good deal of code which is moved into Peer to avoid duplication.
 *
 * 继承关系: 子类Follower和Observer
 * 内部类 ：PacketInFlight 表示尚在PROPOSAL还未COMMIT的消息记录
 * 属性
 * 方法
 *   与leader交互相关
 *     findLeader：找到leader，发现地址
 *     connectToLeader：和leader建立连接
 *     registerWithLeader：向leader注册，发送learnerInfo
 *     syncWithLeader：启动时learner先和leader进行数据同步
 *   读写packet
 *   session验证相关
 *     validateSession:集群版client重连时调用，learner验证会话是否有效，并激活，需要发送请求给Leader
 *     revalidate:接收到了leader返回的REVALIDATE信息，进行验证处理
 *   其他
 *     request:所有请求都被LEADER处理，learner接收到请求会转发给Leader
 *     ping:learner接收leader的ping命令时，返回LearnerSessionTracker的快照
 */
public class Learner {       
    static class PacketInFlight {
        TxnHeader hdr;  //事务头
        Record rec;     //记录
    }
    QuorumPeer self;             //集群本身对象
    LearnerZooKeeperServer zk;   //当前learn状态的zk服务器
    
    protected BufferedOutputStream bufferedOutput;
    
    protected Socket sock;
    
    /**
     * Socket getter
     * @return 
     */
    public Socket getSocket() {
        return sock;
    }
    
    protected InputArchive leaderIs;             //输入
    protected OutputArchive leaderOs;            //输出
    /** the protocol version of the leader */
    protected int leaderProtocolVersion = 0x01;  //当前协议版本
    
    protected static final Logger LOG = LoggerFactory.getLogger(Learner.class);

    static final private boolean nodelay = System.getProperty("follower.nodelay", "true").equals("true"); //链接leader是否允许延迟
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }   
    
    final ConcurrentHashMap<Long, ServerCnxn> pendingRevalidations = new ConcurrentHashMap<Long, ServerCnxn>();//client连接到learner时，learner要向leader提出REVALIDATE请求，在收到回复之前，记录在一个map中，表示尚未处理完的验证
    
    public int getPendingRevalidationsCount() {
        return pendingRevalidations.size();
    }
    
    /**
     * validate a session for a client
     *
     * @param clientId
     *                the client to be revalidated
     * @param timeout
     *                the timeout for which the session is valid
     * @return
     * @throws IOException
     * 验证session
     */
    void validateSession(ServerCnxn cnxn, long clientId, int timeout)
            throws IOException {
        LOG.info("Revalidating client: 0x" + Long.toHexString(clientId));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(clientId);
        dos.writeInt(timeout);
        dos.close();
        QuorumPacket qp = new QuorumPacket(Leader.REVALIDATE, -1, baos
                .toByteArray(), null);
        pendingRevalidations.put(clientId, cnxn);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                                     ZooTrace.SESSION_TRACE_MASK,
                                     "To validate session 0x"
                                     + Long.toHexString(clientId));
        }
        writePacket(qp, true);
    }     
    
    /**
     * write a packet to the leader
     *
     * @param pp
     *                the proposal packet to be sent to the leader
     * @throws IOException
     */
    void writePacket(QuorumPacket pp, boolean flush) throws IOException {
        synchronized (leaderOs) {
            if (pp != null) {
                leaderOs.writeRecord(pp, "packet");
            }
            if (flush) {
                bufferedOutput.flush();
            }
        }
    }

    /**
     * read a packet from the leader
     *
     * @param pp
     *                the packet to be instantiated
     * @throws IOException
     */
    void readPacket(QuorumPacket pp) throws IOException {
        synchronized (leaderIs) {
            leaderIs.readRecord(pp, "packet");
        }
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        if (pp.getType() == Leader.PING) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logQuorumPacket(LOG, traceMask, 'i', pp);
        }
    }
    
    /**
     * send a request packet to the leader
     *
     * @param request
     *                the request from the client
     * @throws IOException
     * learen所有的请求都会转发给leader
     */
    void request(Request request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream oa = new DataOutputStream(baos);
        oa.writeLong(request.sessionId);
        oa.writeInt(request.cxid);
        oa.writeInt(request.type);
        if (request.request != null) {
            request.request.rewind();
            int len = request.request.remaining();
            byte b[] = new byte[len];
            request.request.get(b);
            request.request.rewind();
            oa.write(b);
        }
        oa.close();
        QuorumPacket qp = new QuorumPacket(Leader.REQUEST, -1, baos
                .toByteArray(), request.authInfo);
        writePacket(qp, true);
    }
    
    /**
     * Returns the address of the node we think is the leader.
     */
    protected QuorumServer findLeader() {
        QuorumServer leaderServer = null;
        // Find the leader by id
        Vote current = self.getCurrentVote();
        for (QuorumServer s : self.getView().values()) {
            if (s.id == current.getId()) {
                // Ensure we have the leader's correct IP address before
                // attempting to connect.
                s.recreateSocketAddresses();
                leaderServer = s;
                break;
            }
        }
        if (leaderServer == null) {
            LOG.warn("Couldn't find the leader with id = "
                    + current.getId());
        }
        return leaderServer;
    }
    
    /**
     * Establish a connection with the Leader found by findLeader. Retries
     * 5 times before giving up. 
     * @param addr - the address of the Leader to connect to.
     * @throws IOException <li>if the socket connection fails on the 5th attempt</li>
     * <li>if there is an authentication failure while connecting to leader</li>
     * @throws ConnectException
     * @throws InterruptedException
     * 和leader建立连接
     */
    protected void connectToLeader(InetSocketAddress addr, String hostname)
            throws IOException, ConnectException, InterruptedException {
        sock = new Socket();
        sock.setSoTimeout(self.tickTime * self.initLimit);
        for (int tries = 0; tries < 5; tries++) {
            try {
                sock.connect(addr, self.tickTime * self.syncLimit);
                sock.setTcpNoDelay(nodelay);
                break;
            } catch (IOException e) {
                if (tries == 4) {
                    LOG.error("Unexpected exception",e);
                    throw e;
                } else {
                    LOG.warn("Unexpected exception, tries="+tries+
                            ", connecting to " + addr,e);
                    sock = new Socket();
                    sock.setSoTimeout(self.tickTime * self.initLimit);
                }
            }
            Thread.sleep(1000);
        }

        self.authLearner.authenticate(sock, hostname);

        leaderIs = BinaryInputArchive.getArchive(new BufferedInputStream(
                sock.getInputStream()));
        bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
        leaderOs = BinaryOutputArchive.getArchive(bufferedOutput);
    }   
    
    /**
     * Once connected to the leader, perform the handshake protocol to
     * establish a following / observing connection. 
     * @param pktType
     * @return the zxid the Leader sends for synchronization purposes.
     * @throws IOException
     * Learner完成和Leader的连接后，会向Leader进行注册，
     * 即将Learner服务器的基本信息（LearnerInfo），
     * 包括SID和ZXID，发送给Leader服务器。
     */
    protected long registerWithLeader(int pktType) throws IOException{//注册当前的follower
        /*
         * Send follower info, including last zxid and sid
         */
    	long lastLoggedZxid = self.getLastLoggedZxid();//获取自身最新的zxid
        QuorumPacket qp = new QuorumPacket();                
        qp.setType(pktType);//设置类型为Leader.FOLLOWERINFO或者Leader.OBSERVERINFO
        qp.setZxid(ZxidUtils.makeZxid(self.getAcceptedEpoch(), 0));//设置zxid
        
        /*
         * Add sid to payload
         */
        LearnerInfo li = new LearnerInfo(self.getId(), 0x10000);//add 自己的sid
        ByteArrayOutputStream bsid = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(bsid);
        boa.writeRecord(li, "LearnerInfo"); //把learner当前信息发送给leader
        qp.setData(bsid.toByteArray());
        writePacket(qp, true); //发送learnInfo包
        readPacket(qp);//接收leader的回复(新版本是一个LEADERINFO的消息，包含leader的状态)
        final long newEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());//获取newEpoch
		if (qp.getType() == Leader.LEADERINFO) { //新版本的Leader
        	// we are connected to a 1.0 server so accept the new epoch and read the next packet
            //已经连接到新服务器了,接收新的newEpoch 并发送下个包 ack包
        	leaderProtocolVersion = ByteBuffer.wrap(qp.getData()).getInt();
        	byte epochBytes[] = new byte[4];
        	final ByteBuffer wrappedEpochBytes = ByteBuffer.wrap(epochBytes);
        	if (newEpoch > self.getAcceptedEpoch()) {
        		wrappedEpochBytes.putInt((int)self.getCurrentEpoch());
        		self.setAcceptedEpoch(newEpoch);
        	} else if (newEpoch == self.getAcceptedEpoch()) {
                //Epoch相等,需要发送lastLoggedZxid同步
                wrappedEpochBytes.putInt(-1);
        	} else {
        		throw new IOException("Leaders epoch, " + newEpoch + " is less than accepted epoch, " + self.getAcceptedEpoch());
        	}
        	//发送ack
        	QuorumPacket ackNewEpoch = new QuorumPacket(Leader.ACKEPOCH, lastLoggedZxid, epochBytes, null);
        	writePacket(ackNewEpoch, true);
            return ZxidUtils.makeZxid(newEpoch, 0);
        } else {//老版本的Leader
        	if (newEpoch > self.getAcceptedEpoch()) {
        		self.setAcceptedEpoch(newEpoch);
        	}
            if (qp.getType() != Leader.NEWLEADER) {
                LOG.error("First packet should have been NEWLEADER");
                throw new IOException("First packet should have been NEWLEADER");
            }
            return qp.getZxid();
        }
    } 
    
    /**
     * Finally, synchronize our history with the Leader. 
     * @param newLeaderZxid
     * @throws IOException
     * @throws InterruptedException
     * 启动是
     */
    protected void syncWithLeader(long newLeaderZxid) throws IOException, InterruptedException{//和server同步
        QuorumPacket ack = new QuorumPacket(Leader.ACK, 0, null, null);//ack包
        QuorumPacket qp = new QuorumPacket();
        long newEpoch = ZxidUtils.getEpochFromZxid(newLeaderZxid);
        // In the DIFF case we don't need to do a snapshot because the transactions will sync on top of any existing snapshot
        // For SNAP and TRUNC the snapshot is needed to save that history
        boolean snapshotNeeded = true;
        readPacket(qp);
        LinkedList<Long> packetsCommitted = new LinkedList<Long>();
        LinkedList<PacketInFlight> packetsNotCommitted = new LinkedList<PacketInFlight>(); //收到proposal但是还未commit的包
        synchronized (zk) {
            if (qp.getType() == Leader.DIFF) {//接收到diff信息 表示以diff方式与leader的数据同步
                LOG.info("Getting a diff from the leader 0x{}", Long.toHexString(qp.getZxid()));
                snapshotNeeded = false;
            }
            else if (qp.getType() == Leader.SNAP) {//表示snap方式与leader同步,从leader复制一份镜像数据到本地内存
                LOG.info("Getting a snapshot from leader 0x" + Long.toHexString(qp.getZxid()));
                // The leader is going to dump the database
                // clear our own database and read
                zk.getZKDatabase().clear();
                zk.getZKDatabase().deserializeSnapshot(leaderIs);////从leader复制一份镜像数据到本地内存
                String signature = leaderIs.readString("signature");
                if (!signature.equals("BenWasHere")) {
                    LOG.error("Missing signature. Got " + signature);
                    throw new IOException("Missing signature");                   
                }
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());
            } else if (qp.getType() == Leader.TRUNC) {//触发回滚，回滚到leader的lastzxid
                //we need to truncate the log to the lastzxid of the leader
                LOG.warn("Truncating log to get in sync with the leader 0x"
                        + Long.toHexString(qp.getZxid()));
                boolean truncated=zk.getZKDatabase().truncateLog(qp.getZxid());
                if (!truncated) {
                    // not able to truncate the log
                    LOG.error("Not able to truncate the log "
                            + Long.toHexString(qp.getZxid()));
                    System.exit(13);
                }
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());
            }
            else {
                LOG.error("Got unexpected packet from leader "
                        + qp.getType() + " exiting ... " );
                System.exit(13);

            }
            zk.createSessionTracker();//创建session追踪
            
            long lastQueued = 0;

            // in Zab V1.0 (ZK 3.4+) we might take a snapshot when we get the NEWLEADER message, but in pre V1.0
            // we take the snapshot on the UPDATE message, since Zab V1.0 also gets the UPDATE (after the NEWLEADER)
            // we need to make sure that we don't take the snapshot twice.
            boolean isPreZAB1_0 = true; //
            //If we are not going to take the snapshot be sure the transactions are not applied in memory
            // but written out to the transaction log
            boolean writeToTxnLog = !snapshotNeeded;//标识是否接受到了 snapshotTaken 请求
            // we are now going to start getting transactions to apply followed by an UPTODATE
            outerLoop:
            /**
             * 1.前面registerWithLeader函数learner会回复leader的LEADERINFO,带上了自己的lastLoggedZxid
             * 2.leader根据lastLoggedZxid告诉learner是哪一种同步方式
             *   DIFF同步,还是SNAP同步,还是先TRUNC回滚到某个zxid
             * 3.确定同步方式之后，leader会接着给learner发送后续的同步packet，分为
             * PROPOSAL（提议）
             * COMMIT（提交，针对Follower)
             * INFORM（通知，针对Observer)
             * UPTODATE(表示过半机器已完成同步，可以对外工作)
             * NEWLEADER(leader告诉learner同步的相关请求已经发完了)
             *
             *
             * 1.DIFF 和 SNAP 两种数据同步的方式的区别是什么
             * 打个比方,LEADER的样子是A，LEARNER的样式是B
             * DIFF：代表LEADER后续会告诉LEARNER：不断接收PROPOSAL和COMMIT，一步步提交让LEARNER从B变成B1，到B2。。。最后到A的样子.
             * SNAP：代表LEADER直接告诉LEARNER:我长得样子是A，你copy一下变成我的样子就好了
             *
             * 2.是否发送了DIFF ,SNAP或者TRUNC，learner接受到一下就一下子数据同步完成了
             * 不是
             * DIFF：后面会跟着一堆INFORM和COMMIT让learner一步步的同步
             * TRUNC：回滚到某个zxid后，后面也会跟着一堆INFORM和COMMIT
             * SNAP：后面会发送自己db的序列化内容
             */
            while (self.isRunning()) {//启动时数据同步,不断读取leader的数据，直到收到UPTODATE表示同步完成
                 readPacket(qp);//
                switch(qp.getType()) {
                case Leader.PROPOSAL://接收到提议
                    PacketInFlight pif = new PacketInFlight();
                    pif.hdr = new TxnHeader(); //header
                    pif.rec = SerializeUtils.deserializeTxn(qp.getData(), pif.hdr);//记录
                    if (pif.hdr.getZxid() != lastQueued + 1) {
                    LOG.warn("Got zxid 0x"
                            + Long.toHexString(pif.hdr.getZxid())
                            + " expected 0x"
                            + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = pif.hdr.getZxid();
                    packetsNotCommitted.add(pif);////因为还没有commit，记录在notCommit的队列里面
                    break;
                case Leader.COMMIT://接收到commit
                    if (!writeToTxnLog) {//是否拍过快照
                        pif = packetsNotCommitted.peekFirst();
                        if (pif.hdr.getZxid() != qp.getZxid()) {
                            LOG.warn("Committing " + qp.getZxid() + ", but next proposal is " + pif.hdr.getZxid());
                        } else {
                            zk.processTxn(pif.hdr, pif.rec);//直接处理事务
                            packetsNotCommitted.remove();//从未提交事务中移除
                        }
                    } else {
                        packetsCommitted.add(qp.getZxid());//直接加入
                    }
                    break;
                case Leader.INFORM: ///observer才会拿到INFORM消息，来同步
                    /*
                     * Only observer get this type of packet. We treat this
                     * as receiving PROPOSAL and COMMMIT.
                     */
                    PacketInFlight packet = new PacketInFlight();
                    packet.hdr = new TxnHeader();
                    packet.rec = SerializeUtils.deserializeTxn(qp.getData(), packet.hdr);
                    // Log warning message if txn comes out-of-order
                    if (packet.hdr.getZxid() != lastQueued + 1) {
                        LOG.warn("Got zxid 0x"
                                + Long.toHexString(packet.hdr.getZxid())
                                + " expected 0x"
                                + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = packet.hdr.getZxid();
                    if (!writeToTxnLog) {
                        // Apply to db directly if we haven't taken the snapshot
                        zk.processTxn(packet.hdr, packet.rec);
                    } else {
                        packetsNotCommitted.add(packet);
                        packetsCommitted.add(qp.getZxid());
                    }
                    break;
                case Leader.UPTODATE:////过半机器完成了leader验证，自己也完成了数据同步,可以跳出循环
                    if (isPreZAB1_0) {
                        zk.takeSnapshot();
                        self.setCurrentEpoch(newEpoch);
                    }
                    self.cnxnFactory.setZooKeeperServer(zk);                
                    break outerLoop;
                case Leader.NEWLEADER: // Getting NEWLEADER here instead of in discovery 
                    // means this is Zab 1.0
                    // Create updatingEpoch file and remove it after current
                    // epoch is set. QuorumPeer.loadDataBase() uses this file to
                    // detect the case where the server was terminated after
                    // taking a snapshot but before setting the current epoch.
                    //在此处获得NEWLEADER而不是在发现中
                    ///表示这是Zab 1.0
                    // 创建updateEpoch文件并在设置当前历元之后将其删除。 QuorumPeer.loadDataBase（）使用此文件
                    // 检测在拍摄快照之后设置当前历元之前终止服务器的情况。
                    File updating = new File(self.getTxnFactory().getSnapDir(),
                                        QuorumPeer.UPDATING_EPOCH_FILENAME);
                    if (!updating.exists() && !updating.createNewFile()) {
                        throw new IOException("Failed to create " +
                                              updating.toString());
                    }
                    if (snapshotNeeded) {
                        zk.takeSnapshot();
                    }
                    self.setCurrentEpoch(newEpoch);
                    if (!updating.delete()) {
                        throw new IOException("Failed to delete " +
                                              updating.toString());
                    }
                    writeToTxnLog = true; //Anything after this needs to go to the transaction log, not applied directly in memory
                    isPreZAB1_0 = false;
                    writePacket(new QuorumPacket(Leader.ACK, newLeaderZxid, null, null), true);
                    break;
                }
            }
        }
        ack.setZxid(ZxidUtils.makeZxid(newEpoch, 0));
        writePacket(ack, true);//最后在发一个ack
        sock.setSoTimeout(self.tickTime * self.syncLimit);
        zk.startup();//启动服务器
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(newEpoch);//更新选举投票

        // We need to log the stuff that came in between the snapshot and the uptodate
        if (zk instanceof FollowerZooKeeperServer) {//follower
            FollowerZooKeeperServer fzk = (FollowerZooKeeperServer)zk;
            for(PacketInFlight p: packetsNotCommitted) {
                fzk.logRequest(p.hdr, p.rec);
            }
            for(Long zxid: packetsCommitted) {
                fzk.commit(zxid);//进行commit
            }
        } else if (zk instanceof ObserverZooKeeperServer) { //如果是
            // Similar to follower, we need to log requests between the snapshot
            // and UPTODATE
            ObserverZooKeeperServer ozk = (ObserverZooKeeperServer) zk;
            for (PacketInFlight p : packetsNotCommitted) {
                Long zxid = packetsCommitted.peekFirst();
                if (p.hdr.getZxid() != zxid) {
                    // log warning message if there is no matching commit
                    // old leader send outstanding proposal to observer
                    LOG.warn("Committing " + Long.toHexString(zxid)
                            + ", but next proposal is "
                            + Long.toHexString(p.hdr.getZxid()));
                    continue;
                }
                packetsCommitted.remove();
                Request request = new Request(null, p.hdr.getClientId(),
                        p.hdr.getCxid(), p.hdr.getType(), null, null);
                request.txn = p.rec;
                request.hdr = p.hdr;
                ozk.commitRequest(request);
            }
        } else {
            // New server type need to handle in-flight packets
            throw new UnsupportedOperationException("Unknown server type");
        }
    }
    
    protected void revalidate(QuorumPacket qp) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(qp
                .getData());
        DataInputStream dis = new DataInputStream(bis);
        long sessionId = dis.readLong();
        boolean valid = dis.readBoolean();
        ServerCnxn cnxn = pendingRevalidations
        .remove(sessionId);
        if (cnxn == null) {
            LOG.warn("Missing session 0x"
                    + Long.toHexString(sessionId)
                    + " for validation");
        } else {
            zk.finishSessionInit(cnxn, valid);
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                    ZooTrace.SESSION_TRACE_MASK,
                    "Session 0x" + Long.toHexString(sessionId)
                    + " is valid: " + valid);
        }
    }

    /**
     * learner接收leader的ping命令时，返回LearnerSessionTracker的快照
     * @param qp
     * @throws IOException
     */
    protected void ping(QuorumPacket qp) throws IOException {
        // Send back the ping with our session data
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        HashMap<Long, Integer> touchTable = zk
                .getTouchSnapshot();
        for (Entry<Long, Integer> entry : touchTable.entrySet()) {
            dos.writeLong(entry.getKey());
            dos.writeInt(entry.getValue());
        }
        qp.setData(bos.toByteArray());
        writePacket(qp, true);
    }
    
    
    /**
     * Shutdown the Peer
     */
    public void shutdown() {
        // set the zookeeper server to null
        self.cnxnFactory.setZooKeeperServer(null);
        // clear all the connections
        self.cnxnFactory.closeAll();
        // shutdown previous zookeeper
        if (zk != null) {
            zk.shutdown();
        }
    }

    boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }
}
