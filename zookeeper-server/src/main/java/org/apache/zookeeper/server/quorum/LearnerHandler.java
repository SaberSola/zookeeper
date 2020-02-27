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
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import javax.security.sasl.SaslException;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 * 为了保证整个集群内部的实时通信，同时为了确保可以控制所有的Follower/Observer服务器，Leader服务器会与每个Follower/Observer服务器建立一个TCP长连接。
 * 同时也会为每个Follower/Observer服务器创建一个名为LearnerHandler的实体。LearnerHandler是Learner服务器的管理者，
 * 主要负责Follower/Observer服务器和Leader服务器之间的一系列网络通信，包括数据同步、请求转发和Proposal提议的投票等。Leader服务器中保存了所有Follower/Observer对应的LearnerHandler。
 *
 * 简介
 * 内部类
 *   SyncLimitCheck：控制leader等待当前learner给proposal回复ACK的时间
 * 属性
 *   tickOfNextAckDeadline：下次回复ACK的deadline(周期数，不是时间)
 * 方法
 *   构造函数
 *   发送packet
 *   验证，关闭相关
 *     shutdown：关闭handler
 *     ping：检测leader与learner是否有proposal超时了，发出ping命令
 *     synced：是否同步，根据tickOfNextAckDeadline和线程状态判断
 *   线程方法run：解析Learner信息，发送Leader状态，完成数据同步，启动Leader服务器，和Learner正常交互
 */
public class LearnerHandler extends ZooKeeperThread { //表示leader个learener的交互线程
    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);

    protected final Socket sock;//socket

    public Socket getSocket() {
        return sock;
    }

    final Leader leader;//对应的leader

    /** Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this learner should
     * be considered no longer "sync'd" with the leader. */
    volatile long tickOfNextAckDeadline; //下一个接收ack的deadline，启动时(数据同步)是一个标准，完成启动后(正常交互)，是另一个标准
    
    /**
     * ZooKeeper server identifier of this learner
     */
    protected long sid = 0;//learner的sid

    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket(); //代表一个关闭shutdown的packet来关闭发送packet的线程

    private LearnerType  learnerType = LearnerType.PARTICIPANT; ////默认的learner类型（也叫Follower）,也可以设置为OBSERVER

    private SyncLimitCheck syncLimitCheck = new SyncLimitCheck(); ////proposal，ack检测

    private BinaryInputArchive ia;

    private BinaryOutputArchive oa;

    private final BufferedInputStream bufferedInput;

    private BufferedOutputStream bufferedOutput;
    
    long getSid(){
        return sid;
    }                    

    protected int version = 0x1;//learner的version
    
    int getVersion() {
    	return version;
    }
    
    /**
     * The packets to be sent to the learner
     */
    final LinkedBlockingQueue<QuorumPacket> queuedPackets = new LinkedBlockingQueue<QuorumPacket>();//待发送packet的队列

    /**
     * This class controls the time that the Leader has been
     * waiting for acknowledgement of a proposal from this Learner.
     * If the time is above syncLimit, the connection will be closed.
     * It keeps track of only one proposal at a time, when the ACK for
     * that proposal arrives, it switches to the last proposal received
     * or clears the value if there is no pending proposal.
     * 作用就是控制leader等待当前learner给proposal回复ACK的时间
     *
     * 在Leader发出proposal时更新对应时间，zxid记录
     * 在Leader收到对应ACK时，清除对应zxid的记录
     * 检查时，判断当前时间和最早已经发出proposal但是没有收到ack的时间对比，看是否超时
     *
     */
    private class SyncLimitCheck {
        private boolean started = false;
        private long currentZxid = 0;//最久一次更新了但是没有收到ack的proposal的zxid
        private long currentTime = 0;//最久一次更新了但是没有收到ack的proposal的时间
        private long nextZxid = 0;//最新一次更新了但是没有收到ack的proposal的zxid
        private long nextTime = 0; //最新一次更新了但是没有收到ack的proposal的时间

        //启动检查
        public synchronized void start() {
            started = true;
        }

        //发送proposal时，更新提议的统计时间
        public synchronized void updateProposal(long zxid, long time) {
            if (!started) {
                return;
            }
            //如果没初始化就初始化
            if (currentTime == 0) {
                currentTime = time;
                currentZxid = zxid;
            } else { //已经初始化就记录下下次的时间和zxid
                nextTime = time;
                nextZxid = zxid;
            }
        }

        public synchronized void updateAck(long zxid) {//收到Learner关于zxid的ack了，更新ack的统计时间
             if (currentZxid == zxid) {//如果是刚刚发送的ack 传递到下一个记录
                 currentTime = nextTime;
                 currentZxid = nextZxid;
                 nextTime = 0;
                 nextZxid = 0;
             } else if (nextZxid == zxid) {//如果旧的ack还没收到 但是收到了 新的ack
                 LOG.warn("ACK for " + zxid + " received before ACK for " + currentZxid + "!!!!");
                 nextTime = 0;
                 nextZxid = 0;
             }
        }

        public synchronized boolean check(long time) {////如果没有等待超时，返回true
            if (currentTime == 0) {
                return true;
            } else {
                long msDelay = (time - currentTime) / 1000000;//当前时间与最久一次没收到ack的proposal的时间差
                return (msDelay < (leader.self.tickTime * leader.self.syncLimit));
            }
        }
    };


    LearnerHandler(Socket sock, BufferedInputStream bufferedInput,
                   Leader leader) throws IOException {
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.leader = leader;
        this.bufferedInput = bufferedInput;
        try {
            leader.self.authServer.authenticate(sock,
                    new DataInputStream(bufferedInput));
        } catch (IOException e) {
            LOG.error("Server failed to authenticate quorum learner, addr: {}, closing connection",
                    sock.getRemoteSocketAddress(), e);
            try {
                sock.close();
            } catch (IOException ie) {
                LOG.error("Exception while closing socket", ie);
            }
            throw new SaslException("Authentication failure: " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfNextAckDeadline:").append(tickOfNextAckDeadline());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     *
     * @throws InterruptedException
     * 消费队列
     */
    private void sendPackets() throws InterruptedException {
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        while (true) {
            try {
                QuorumPacket p;
                p = queuedPackets.poll();//从队列获取
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }

                if (p == proposalOfDeath) {//调用shutdown
                    // Packet of death!
                    break;
                }
                if (p.getType() == Leader.PING) { //ping
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (p.getType() == Leader.PROPOSAL) {//更新当前proposal的时间统计
                    syncLimitCheck.updateProposal(p.getZxid(), System.nanoTime());
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }
                oa.writeRecord(p, "packet");
            } catch (IOException e) {
                if (!sock.isClosed()) {
                    LOG.warn("Unexpected exception at " + this, e);
                    try {
                        // this will cause everything to shutdown on
                        // this learner handler and will help notify
                        // the learner/observer instantaneously
                        sock.close();
                    } catch(IOException ie) {
                        LOG.warn("Error closing socket for handler " + this, ie);
                    }
                }
                break;
            }
        }
    }

    static public String packetToString(QuorumPacket p) {
        String type = null;
        String mess = null;
        Record txn = null;
        
        switch (p.getType()) {
        case Leader.ACK:
            type = "ACK";
            break;
        case Leader.COMMIT:
            type = "COMMIT";
            break;
        case Leader.FOLLOWERINFO:
            type = "FOLLOWERINFO";
            break;    
        case Leader.NEWLEADER:
            type = "NEWLEADER";
            break;
        case Leader.PING:
            type = "PING";
            break;
        case Leader.PROPOSAL:
            type = "PROPOSAL";
            TxnHeader hdr = new TxnHeader();
            try {
                SerializeUtils.deserializeTxn(p.getData(), hdr);
                // mess = "transaction: " + txn.toString();
            } catch (IOException e) {
                LOG.warn("Unexpected exception",e);
            }
            break;
        case Leader.REQUEST:
            type = "REQUEST";
            break;
        case Leader.REVALIDATE:
            type = "REVALIDATE";
            ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
            DataInputStream dis = new DataInputStream(bis);
            try {
                long id = dis.readLong();
                mess = " sessionid = " + id;
            } catch (IOException e) {
                LOG.warn("Unexpected exception", e);
            }

            break;
        case Leader.UPTODATE:
            type = "UPTODATE";
            break;
        default:
            type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     */
    @Override
    public void run() {
        try {
            leader.addLearnerHandler(this);
            //leader的当前周期 + 初始化以及同步的limit(initLimit + syncLimit)
            tickOfNextAckDeadline = leader.self.tick.get() + leader.self.initLimit + leader.self.syncLimit;
            ia = BinaryInputArchive.getArchive(bufferedInput);
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);

            QuorumPacket qp = new QuorumPacket();//packet
            ia.readRecord(qp, "packet");
            if(qp.getType() != Leader.FOLLOWERINFO && qp.getType() != Leader.OBSERVERINFO){
            	LOG.error("First packet " + qp.toString()
                        + " is not FOLLOWERINFO or OBSERVERINFO!");
                return;
            }
            byte learnerInfoData[] = qp.getData();//接收learner发出来的learnInfo 包含sid
            if (learnerInfoData != null) {
            	if (learnerInfoData.length == 8) {
            		ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
            		this.sid = bbsid.getLong();
            	} else {
            		LearnerInfo li = new LearnerInfo();
            		ByteBufferInputStream.byteBuffer2Record(ByteBuffer.wrap(learnerInfoData), li);
            		this.sid = li.getServerid();
            		this.version = li.getProtocolVersion();
            	}
            } else {
            	this.sid = leader.followerCounter.getAndDecrement();
            }

            LOG.info("Follower sid: " + sid + " : info : "
                    + leader.self.quorumPeers.get(sid));
                        
            if (qp.getType() == Leader.OBSERVERINFO) {
                  learnerType = LearnerType.OBSERVER;
            }            
            
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());//记录当前最新的接收的epoch
            
            long peerLastZxid;//
            StateSummary ss = null;
            long zxid = qp.getZxid();
            long newEpoch = leader.getEpochToPropose(this.getSid(), lastAcceptedEpoch);//如果learner的epoch比自己高，更新自己的
            
            if (this.getVersion() < 0x10000) {//leader是旧版本
                // we are going to have to extrapolate the epoch information
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                ss = new StateSummary(epoch, zxid);
                // fake the message
                leader.waitForEpochAck(this.getSid(), ss);
            } else {//leader 是新版本
                byte ver[] = new byte[4];
                ByteBuffer.wrap(ver).putInt(0x10000);
                QuorumPacket newEpochPacket = new QuorumPacket(Leader.LEADERINFO, ZxidUtils.makeZxid(newEpoch, 0), ver, null);
                oa.writeRecord(newEpochPacket, "packet");
                bufferedOutput.flush();
                QuorumPacket ackEpochPacket = new QuorumPacket();
                ia.readRecord(ackEpochPacket, "packet"); //接收learner的ACKEPOCH
                if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                    LOG.error(ackEpochPacket.toString()
                            + " is not ACKEPOCH");
                    return;
				}
                ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData());
                ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                leader.waitForEpochAck(this.getSid(), ss);
            }
            peerLastZxid = ss.getLastZxid();
            
            /* the default to send to the follower */
            int packetToSend = Leader.SNAP;
            long zxidToSend = 0;
            long leaderLastZxid = 0;
            /** the packets that the follower needs to get updates from **/
            long updates = peerLastZxid;
            
            /* we are sending the diff check if we have proposals in memory to be able to 
             * send a diff to the 
             */ 
            ReentrantReadWriteLock lock = leader.zk.getZKDatabase().getLogLock();//获取读写锁
            ReadLock rl = lock.readLock();
            try {
                rl.lock();        
                final long maxCommittedLog = leader.zk.getZKDatabase().getmaxCommittedLog();//日志中记录的最大事务Id
                final long minCommittedLog = leader.zk.getZKDatabase().getminCommittedLog();//日志中记录的最小事务Id
                LOG.info("Synchronizing with Follower sid: " + sid
                        +" maxCommittedLog=0x"+Long.toHexString(maxCommittedLog)
                        +" minCommittedLog=0x"+Long.toHexString(minCommittedLog)
                        +" peerLastZxid=0x"+Long.toHexString(peerLastZxid));

                LinkedList<Proposal> proposals = leader.zk.getZKDatabase().getCommittedLog();//获取提交的Proposal, packet的type都是Leader.PROPOSAL

                if (peerLastZxid == leader.zk.getZKDatabase().getDataTreeLastProcessedZxid()) {
                    // Follower is already sync with us, send empty diff
                    LOG.info("leader and follower are in sync, zxid=0x{}",
                            Long.toHexString(peerLastZxid));
                    packetToSend = Leader.DIFF;
                    zxidToSend = peerLastZxid;//learen已经同步过了 但是也发送
                } else if (proposals.size() != 0) {
                    LOG.debug("proposal size is {}", proposals.size());
                    if ((maxCommittedLog >= peerLastZxid)
                            && (minCommittedLog <= peerLastZxid)) {//如果learner的zxid在leader的[minCommittedLog, maxCommittedLog]范围内
                        LOG.debug("Sending proposals to follower");

                        // as we look through proposals, this variable keeps track of previous
                        // proposal Id.
                        long prevProposalZxid = minCommittedLog;//赋值最小的zxid

                        //跟踪我们是否要发送第一个数据包。
                        //在发送第一个数据包之前，我们必须告诉Learener
                        //是否期望截断或差异
                        boolean firstPacket=true;

                        // If we are here, we can use committedLog to sync with
                        // follower. Then we only need to decide whether to
                        // send trunc or not
                        packetToSend = Leader.DIFF;//默认是Diff乐行
                        zxidToSend = maxCommittedLog;

                        for (Proposal propose: proposals) {
                            // skip the proposals the peer already has
                            if (propose.packet.getZxid() <= peerLastZxid) {//leader提交的proposal已经被learner处理过了，那么就跳过
                                prevProposalZxid = propose.packet.getZxid();
                                continue;
                            } else {
                                // If we are sending the first packet, figure out whether to trunc
                                // in case the follower has some proposals that the leader doesn't
                                if (firstPacket) {//第一次发送的pacet
                                    firstPacket = false;
                                    // Does the peer have some proposals that the leader hasn't seen yet
                                    if (prevProposalZxid < peerLastZxid) {//如果learner有一些leader不知道的请求(正常来说应该是prevProposalZxid == peerLastZxid)
                                        // send a trunc message before sending the diff
                                        packetToSend = Leader.TRUNC; //回滚
                                        zxidToSend = prevProposalZxid;//回滚到prevProposalZxid
                                        updates = zxidToSend;
                                    }
                                }
                                queuePacket(propose.packet);////发送PROPOSAL值队列
                                QuorumPacket qcommit = new QuorumPacket(Leader.COMMIT, propose.packet.getZxid(),
                                        null, null);
                                queuePacket(qcommit);//让刚刚的PROPOSAL进行COMMIT，让learner同步
                            }
                        }
                    } else if (peerLastZxid > maxCommittedLog) {//如果peerLastZxid > maxCommittedLog 则回滚到maxCommittedLog
                        LOG.debug("Sending TRUNC to follower zxidToSend=0x{} updates=0x{}",
                                Long.toHexString(maxCommittedLog),
                                Long.toHexString(updates));

                        packetToSend = Leader.TRUNC;
                        zxidToSend = maxCommittedLog;
                        updates = zxidToSend;
                    } else {
                        LOG.warn("Unhandled proposal scenario");
                    }
                } else {
                    // just let the state transfer happen
                    LOG.debug("proposals is empty");
                }               

                LOG.info("Sending " + Leader.getPacketType(packetToSend));
                leaderLastZxid = leader.startForwarding(this, updates);

            } finally {
                rl.unlock();
            }

            //生成NEWLEADER的packet,发给learner代表自己需要同步的信息发完了
             QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER,
                    ZxidUtils.makeZxid(newEpoch, 0), null, null);
             if (getVersion() < 0x10000) {
                oa.writeRecord(newLeaderQP, "packet");
            } else {
                queuedPackets.add(newLeaderQP);//加入发送队列
            }
            bufferedOutput.flush();///发送NEWLEADER消息
            //Need to set the zxidToSend to the latest zxid
            if (packetToSend == Leader.SNAP) {
                zxidToSend = leader.zk.getZKDatabase().getDataTreeLastProcessedZxid();
            }
            oa.writeRecord(new QuorumPacket(packetToSend, zxidToSend, null, null), "packet");
            bufferedOutput.flush();//告诉leader同步方式,如何同步
            
            /* if we are not truncating or sending a diff just send a snapshot */
            if (packetToSend == Leader.SNAP) {//如果发出snap，代表告知learner进行snap方式的数据同步
                LOG.info("Sending snapshot last zxid of peer is 0x"
                        + Long.toHexString(peerLastZxid) + " " 
                        + " zxid of leader is 0x"
                        + Long.toHexString(leaderLastZxid)
                        + "sent zxid of db as 0x" 
                        + Long.toHexString(zxidToSend));
                // Dump data to peer
                leader.zk.getZKDatabase().serializeSnapshot(oa);
                oa.writeString("BenWasHere", "signature");
            }
            bufferedOutput.flush();//发送
            
            // Start sending packets
            new Thread() {
                public void run() {
                    Thread.currentThread().setName(
                            "Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();//不断发送packets直到接受到proposalOfDeath
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption",e);
                    }
                }
            }.start();//启动线程，发送消息
            
            /*
             * Have to wait for the first ACK, wait until 
             * the leader is ready, and only then we can
             * start processing messages.
             */
            qp = new QuorumPacket();
            ia.readRecord(qp, "packet");
            if(qp.getType() != Leader.ACK){//Learner接收到NEWLEADER 一定会返回ACK
                LOG.error("Next packet was supposed to be an ACK");
                return;
            }
            LOG.info("Received NEWLEADER-ACK message from " + getSid());
            leader.waitForNewLeaderAck(getSid(), qp.getZxid());//等待有过半参与者返回ACK

            syncLimitCheck.start();//开始同步超时检查
            
            // now that the ack has been processed expect the syncLimit
            sock.setSoTimeout(leader.self.tickTime * leader.self.syncLimit);//请求阶段的读取超时时间 为 tickTime * syncLimit

            /*
             * Wait until leader starts up
             * 等待直到Leader start up
             */
            synchronized(leader.zk){
                while(!leader.zk.isRunning() && !this.isInterrupted()){
                    leader.zk.wait(20);
                }
            }
            // Mutation packets will be queued during the serialize,
            // so we need to mark when the peer can actually start
            // using the data
            //
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null)); //发送update代表过半的机器回复了NEWLEADER的ACK

            while (true) { //正常交互 等待learener的请求等
                qp = new QuorumPacket();
                ia.readRecord(qp, "packet");

                long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                if (qp.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', qp);
                }
                tickOfNextAckDeadline = leader.self.tick.get() + leader.self.syncLimit;


                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;

                switch (qp.getType()) {
                case Leader.ACK:
                    if (this.learnerType == LearnerType.OBSERVER) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received ACK from Observer  " + this.sid);
                        }
                    }
                    syncLimitCheck.updateAck(qp.getZxid());
                    //处理ack消息
                    leader.processAck(this.sid, qp.getZxid(), sock.getLocalSocketAddress());
                    break;
                case Leader.PING:
                    // Process the touches
                    ByteArrayInputStream bis = new ByteArrayInputStream(qp
                            .getData());
                    DataInputStream dis = new DataInputStream(bis);
                    while (dis.available() > 0) {
                        long sess = dis.readLong();
                        int to = dis.readInt();
                        leader.zk.touch(sess, to); ////会话管理，激活
                    }
                    break;
                case Leader.REVALIDATE:
                    bis = new ByteArrayInputStream(qp.getData());
                    dis = new DataInputStream(bis);
                    long id = dis.readLong();
                    int to = dis.readInt();
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(bos);
                    dos.writeLong(id);
                    boolean valid = leader.zk.touch(id, to);
                    if (valid) {
                        try {
                            //set the session owner
                            // as the follower that
                            // owns the session
                            leader.zk.setOwner(id, this);
                        } catch (SessionExpiredException e) {
                            LOG.error("Somehow session " + Long.toHexString(id) + " expired right after being renewed! (impossible)", e);
                        }
                    }
                    if (LOG.isTraceEnabled()) {
                        ZooTrace.logTraceMessage(LOG,
                                                 ZooTrace.SESSION_TRACE_MASK,
                                                 "Session 0x" + Long.toHexString(id)
                                                 + " is valid: "+ valid);
                    }
                    dos.writeBoolean(valid);
                    qp.setData(bos.toByteArray());
                    queuedPackets.add(qp);
                    break;
                case Leader.REQUEST:                    
                    bb = ByteBuffer.wrap(qp.getData());
                    sessionId = bb.getLong();
                    cxid = bb.getInt();
                    type = bb.getInt();
                    bb = bb.slice();
                    Request si;
                    if(type == OpCode.sync){
                        si = new LearnerSyncRequest(this, sessionId, cxid, type, bb, qp.getAuthinfo());
                    } else {
                        si = new Request(null, sessionId, cxid, type, bb, qp.getAuthinfo());
                    }
                    si.setOwner(this);
                    leader.zk.submitRequest(si);
                    break;
                default:
                    LOG.warn("unexpected quorum packet, type: {}", packetToString(qp));
                    break;
                }
            }
        } catch (IOException e) {
            if (sock != null && !sock.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock "
                        + "still open", e);
            	//close the socket to make sure the 
            	//other side can see it being close
            	try {
            		sock.close();
            	} catch(IOException ie) {
            		// do nothing
            	}
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception causing shutdown", e);
        } finally {
            LOG.warn("******* GOODBYE " 
                    + (sock != null ? sock.getRemoteSocketAddress() : "<null>")
                    + " ********");
            shutdown();
        }
    }

    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }
        try {
            if (sock != null && !sock.isClosed()) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during socket close", e);
        }
        this.interrupt();
        leader.removeLearnerHandler(this);
    }

    public long tickOfNextAckDeadline() {
        return tickOfNextAckDeadline;
    }

    /**
     * ping calls from the leader to the peers
     */

    //发送ping命令，本质就是检测leader与learner是否有proposal超时了(超过指定时长没有收到ack)
    public void ping() {
        long id;
        if (syncLimitCheck.check(System.nanoTime())) {
            synchronized(leader) {
                id = leader.lastProposed;
            }
            QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
            queuePacket(ping);
        } else {
            LOG.warn("Closing connection to peer due to transaction timeout.");
            shutdown();
        }
    }

    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
    }

    //是否保持同步
    public boolean synced() {
        return isAlive()
        && leader.self.tick.get() <= tickOfNextAckDeadline;////线程活着，且当前周期数<deadline
    }
}
