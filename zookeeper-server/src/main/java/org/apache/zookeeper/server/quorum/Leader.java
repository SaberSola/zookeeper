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

import java.io.ByteArrayOutputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.sasl.SaslException;

import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has the control logic for the Leader.
 * 内部类
 *   Proposal，提议的数据结构
 *   ToBeAppliedRequestProcessor
 *   XidRolloverException
 *   LearnerCnxAcceptor,线程，监听Learner连接，启动LearnerHandler不断交互
 * 属性
 *   消息类型相关
 *   其他
 * 函数
 *   启动期相关函数
 *     lead，入口函数
 *     getEpochToPropose：获取集群最大的lastAcceptedEpoch，+1用于设置新的acceptedEpoch
 *     waitForEpochAck：等待过半机器(Learner和leader)针对Leader发出的LEADERINFO回复ACKEPOCH
 *     waitForNewLeaderAck：等到有过半的参与者针对Leader发出的NEWLEADER返回ACK
 *     startZkServer：启动zk
 *   运行期处理相关
 *     处理ACK
 *       processAck：针对提议回复ACK的处理逻辑，如果过半验证了就通知所有Learner
 *       commit，sendPacket：针对提议的确认，给参与者进行通知
 *       inform，sendObserverPacket：针对提议的确认，给观察者进行通知
 *     处理提议
 *       propose:根据Request产生提议,发给所有参与者
 *     处理同步请求
 *       processSync：处理同步请求（暂时不懂）
 *       sendSync:发送Sync请求给合适的server
 *   LearnerHandler相关:
 *     startForwarding：启动交互时，把内存中的提议，即将生效的提议(都还没记录在事务日志当中)告诉给Learner
 */
public class Leader {
    private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
    
    static final private boolean nodelay = System.getProperty("leader.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }

    static public class Proposal { //提议内部类
        public QuorumPacket packet; //集群间传递的包

        public HashSet<Long> ackSet = new HashSet<Long>();//接收到ack的机器sid集合

        public Request request;//请求

        @Override
        public String toString() {
            return packet.getType() + ", " + packet.getZxid() + ", " + request;
        }
    }

    final LeaderZooKeeperServer zk;    //leader的zk server

    final QuorumPeer self;//集群实例
    // VisibleForTesting
    protected boolean quorumFormed = false;  //是否已有过半参与者确认当前leader并且完成同步
    
    // the follower acceptor thread
    LearnerCnxAcceptor cnxAcceptor;//读取learner消息的线程
    
    // list of all the followers
    private final HashSet<LearnerHandler> learners = new HashSet<LearnerHandler>(); ////learnerHandler集合

    /**
     * Returns a copy of the current learner snapshot
     */
    public List<LearnerHandler> getLearners() {
        synchronized (learners) {
            return new ArrayList<LearnerHandler>(learners);
        }
    }

    // list of followers that are ready to follow (i.e synced with the leader)
    private final HashSet<LearnerHandler> forwardingFollowers = new HashSet<LearnerHandler>();//参与者的LearnerHandler集合

    private final ProposalStats proposalStats;

    public ProposalStats getProposalStats() {
        return proposalStats;
    }

    /**
     * Returns a copy of the current forwarding follower snapshot
     */
    public List<LearnerHandler> getForwardingFollowers() {
        synchronized (forwardingFollowers) {
            return new ArrayList<LearnerHandler>(forwardingFollowers);
        }
    }

    private void addForwardingFollower(LearnerHandler lh) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.add(lh);
        }
    }

    private final HashSet<LearnerHandler> observingLearners = new HashSet<LearnerHandler>(); //观察者LearnerHandler集合
        
    /**
     * Returns a copy of the current observer snapshot
     */
    public List<LearnerHandler> getObservingLearners() {
        synchronized (observingLearners) {
            return new ArrayList<LearnerHandler>(observingLearners);
        }
    }

    private void addObserverLearnerHandler(LearnerHandler lh) {
        synchronized (observingLearners) {
            observingLearners.add(lh);
        }
    }

    // Pending sync requests. Must access under 'this' lock.
    private final HashMap<Long,List<LearnerSyncRequest>> pendingSyncs = new HashMap<Long,List<LearnerSyncRequest>>();//正在处理的同步(要等到过半ack才算完)
    
    synchronized public int getNumPendingSyncs() {
        return pendingSyncs.size();
    }

    //Follower counter
    final AtomicLong followerCounter = new AtomicLong(-1); //只不过是一个临时变量，并不是Follower count，看调用方

    /**
     * Adds peer to the leader.
     * 
     * @param learner
     *                instance of learner handle
     */
    void addLearnerHandler(LearnerHandler learner) {
        synchronized (learners) {
            learners.add(learner);
        }
    }

    /**
     * Remove the learner from the learner list
     * 
     * @param peer
     */
    void removeLearnerHandler(LearnerHandler peer) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.remove(peer);            
        }        
        synchronized (learners) {
            learners.remove(peer);
        }
        synchronized (observingLearners) {
            observingLearners.remove(peer);
        }
    }

    boolean isLearnerSynced(LearnerHandler peer){
        synchronized (forwardingFollowers) {
            return forwardingFollowers.contains(peer);
        }        
    }
    
    ServerSocket ss;

    Leader(QuorumPeer self,LeaderZooKeeperServer zk) throws IOException {
        this.self = self;
        this.proposalStats = new ProposalStats();
        try {
            if (self.getQuorumListenOnAllIPs()) {
                ss = new ServerSocket(self.getQuorumAddress().getPort());
            } else {
                ss = new ServerSocket();
            }
            ss.setReuseAddress(true);
            if (!self.getQuorumListenOnAllIPs()) {
                ss.bind(self.getQuorumAddress());
            }
        } catch (BindException e) {
            if (self.getQuorumListenOnAllIPs()) {
                LOG.error("Couldn't bind to port " + self.getQuorumAddress().getPort(), e);
            } else {
                LOG.error("Couldn't bind to " + self.getQuorumAddress(), e);
            }
            throw e;
        }
        this.zk=zk;
    }

    /**
     * This message is for follower to expect diff
     */
    final static int DIFF = 13;
    
    /**
     * This is for follower to truncate its logs 
     */
    final static int TRUNC = 14;
    
    /**
     * This is for follower to download the snapshots
     */
    final static int SNAP = 15;
    
    /**
     * This tells the leader that the connecting peer is actually an observer
     */
    final static int OBSERVERINFO = 16;
    
    /**
     * This message type is sent by the leader to indicate it's zxid and if
     * needed, its database.
     */
    final static int NEWLEADER = 10;

    /**
     * This message type is sent by a follower to pass the last zxid. This is here
     * for backward compatibility purposes.
     */
    final static int FOLLOWERINFO = 11;

    /**
     * This message type is sent by the leader to indicate that the follower is
     * now uptodate andt can start responding to clients.
     */
    final static int UPTODATE = 12;

    /**
     * This message is the first that a follower receives from the leader.
     * It has the protocol version and the epoch of the leader.
     */
    public static final int LEADERINFO = 17;

    /**
     * This message is used by the follow to ack a proposed epoch.
     */
    public static final int ACKEPOCH = 18;
    
    /**
     * This message type is sent to a leader to request and mutation operation.
     * The payload will consist of a request header followed by a request.
     */
    final static int REQUEST = 1;

    /**
     * This message type is sent by a leader to propose a mutation.
     */
    public final static int PROPOSAL = 2;

    /**
     * This message type is sent by a follower after it has synced a proposal.
     */
    final static int ACK = 3;

    /**
     * This message type is sent by a leader to commit a proposal and cause
     * followers to start serving the corresponding data.
     */
    final static int COMMIT = 4;

    /**
     * This message type is enchanged between follower and leader (initiated by
     * follower) to determine liveliness.
     */
    final static int PING = 5;

    /**
     * This message type is to validate a session that should be active.
     */
    final static int REVALIDATE = 6;

    /**
     * This message is a reply to a synchronize command flushing the pipe
     * between the leader and the follower.
     */
    final static int SYNC = 7;
        
    /**
     * This message type informs observers of a committed proposal.
     */
    final static int INFORM = 8;

    ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();//已经提出，还没有处理完的提议的map,key是zxid

    ConcurrentLinkedQueue<Proposal> toBeApplied = new ConcurrentLinkedQueue<Proposal>();//即将生效的提议(已有过半确认)

    Proposal newLeaderProposal = new Proposal();////newLeader的提议


    /**
     * Leader接收到来自其他机器连接创建请求后，会创建一个LearnerHandler实例，
     * 每个LearnerHandler实例都对应一个Leader与Learner服务器之间的连接，
     * 其负责Leader和Learner服务器之间几乎所有的消息通信和数据同步。
     */
    class LearnerCnxAcceptor extends ZooKeeperThread{//用于接收Learener的链接 启动LearnerHandler
        private volatile boolean stop = false;

        public LearnerCnxAcceptor() {
            super("LearnerCnxAcceptor-" + ss.getLocalSocketAddress());
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    try{
                        Socket s = ss.accept();
                        // start with the initLimit, once the ack is processed
                        // in LearnerHandler switch to the syncLimit
                        s.setSoTimeout(self.tickTime * self.initLimit);
                        s.setTcpNoDelay(nodelay);

                        BufferedInputStream is = new BufferedInputStream(
                                s.getInputStream());
                        LearnerHandler fh = new LearnerHandler(s, is, Leader.this);
                        fh.start();
                    } catch (SocketException e) {
                        if (stop) {
                            LOG.info("exception while shutting down acceptor: "
                                    + e);

                            // When Leader.shutdown() calls ss.close(),
                            // the call to accept throws an exception.
                            // We catch and set stop to true.
                            stop = true;
                        } else {
                            throw e;
                        }
                    } catch (SaslException e){
                        LOG.error("Exception while connecting to quorum learner", e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception while accepting follower", e);
            }
        }
        
        public void halt() {
            stop = true;
        }
    }

    StateSummary leaderStateSummary; //leader的状态总结
    
    long epoch = -1;                           //新的acceptEpoch号码,各端的max(lastAcceptedEpoch) + 1, 先默认-1
    boolean waitingForNewEpoch = true;         ////是否在等待新的acceptEpoch号生成
    volatile boolean readyToStart = false;
    
    /**
     * This method is main function that is called to lead
     * 
     * @throws IOException
     * @throws InterruptedException
     * 启动LearnerCnxAcceptor，利用LearnerHandler与各Learner进行IO
     * Leader（以及LearnerHandler）调用getEpochToPropose，接收Leader（以及Learner向Leader注册时发送的LearnerInfo），更新epoch号,作为新的AcceptedEpoch和CurrentEpoch
     * LearnerHandler把发送LEADERINFO包，把上面定的epoch也发送出去
     * 调用waitForEpochAck,等待过半learner(以及leader),针对LEADERINFO返回ACKEPOCH包
     * （省略:LearnHandler中间不断给各Learner同步数据）
     * LearnerHandler发出NEWLEADER，Learner接收到，返回ACK
     * LearnerHandler以及Leader调用waitForNewLeaderAck等待过半PARTICIPANT回复ACK
     * 启动zkServer，不断完成ping，保持过半参与者同步
     */
    void lead() throws IOException, InterruptedException {
        self.end_fle = Time.currentElapsedTime();//master选举的结束时间
        long electionTimeTaken = self.end_fle - self.start_fle;//选举花费的时间
        self.setElectionTimeTaken(electionTimeTaken);
        LOG.info("LEADING - LEADER ELECTION TOOK - {}", electionTimeTaken);
        self.start_fle = 0; //选举开始时间
        self.end_fle = 0;   //选举结束时间

        zk.registerJMX(new LeaderBean(this, zk), self.jmxLocalPeerBean); //注册jmx

        try {
            self.tick.set(0);//初始ticket为0
            zk.loadData();//先loadData加载信息
            
            leaderStateSummary = new StateSummary(self.getCurrentEpoch(), zk.getLastProcessedZxid()); //根据epoch和zxid记录leader的状态

            // Start thread that waits for connection requests from 
            // new followers.
            cnxAcceptor = new LearnerCnxAcceptor();//等到learner的链接
            cnxAcceptor.start();//启动
            
            readyToStart = true;

            long epoch = getEpochToPropose(self.getId(), self.getAcceptedEpoch());//更新epoch
            
            zk.setZxid(ZxidUtils.makeZxid(epoch, 0)); //设置zkServer的zxid
            
            synchronized(this){
                lastProposed = zk.getZxid();//获取zxid
            }
            //生成newLeader包
            newLeaderProposal.packet = new QuorumPacket(NEWLEADER, zk.getZxid(), null, null);


            if ((newLeaderProposal.packet.getZxid() & 0xffffffffL) != 0) {
                LOG.info("NEWLEADER proposal has Zxid of "
                        + Long.toHexString(newLeaderProposal.packet.getZxid()));
            }

            //等待过半learner(以及leader),针对LEADERINFO返回ACKEPOCH包
            waitForEpochAck(self.getId(), leaderStateSummary);
            //设置当前的epoch为epoch
            self.setCurrentEpoch(epoch);

            // We have to get at least a majority of servers in sync with
            // us. We do this by waiting for the NEWLEADER packet to get
            // acknowledged
            try {
                //等到有过半的参与者针对Leader发出的NEWLEADER返回ACK
                waitForNewLeaderAck(self.getId(), zk.getZxid());
            } catch (InterruptedException e) {
                shutdown("Waiting for a quorum of followers, only synced with sids: [ "
                        + getSidSetString(newLeaderProposal.ackSet) + " ]");
                HashSet<Long> followerSet = new HashSet<Long>();
                for (LearnerHandler f : learners)
                    followerSet.add(f.getSid());
                    
                if (self.getQuorumVerifier().containsQuorum(followerSet)) {
                    LOG.warn("Enough followers present. "
                            + "Perhaps the initTicks need to be increased.");
                }
                Thread.sleep(self.tickTime);
                self.tick.incrementAndGet();
                return;
            }
            //启动zk
            startZkServer();
            
            /**
             * WARNING: do not use this for anything other than QA testing
             * on a real cluster. Specifically to enable verification that quorum
             * can handle the lower 32bit roll-over issue identified in
             * ZOOKEEPER-1277. Without this option it would take a very long
             * time (on order of a month say) to see the 4 billion writes
             * necessary to cause the roll-over to occur.
             * 
             * This field allows you to override the zxid of the server. Typically
             * you'll want to set it to something like 0xfffffff0 and then
             * start the quorum, run some operations and see the re-election.
             */

            /**
             * 警告：请勿在实际群集上将其用于QA测试。
             * 专门用于验证Quorum 可以处理ZOOKEEPER-1277中确定的较低的32位翻转问题。如果没有此选项，将需要很长的时间（例如一个月左右）才能看到导致翻转发生的40亿次写入*。
             * 此字段允许您覆盖服务器的zxid。通常，*您需要将其设置为0xfffffff0之类的值，然后*启动仲裁，运行一些操作，然后查看重新选择
             */
            String initialZxid = System.getProperty("zookeeper.testingonly.initialZxid");
            if (initialZxid != null) {
                long zxid = Long.parseLong(initialZxid);
                zk.setZxid((zk.getZxid() & 0xffffffff00000000L) | zxid);
            }
            
            if (!System.getProperty("zookeeper.leaderServes", "yes").equals("no")) {
                self.cnxnFactory.setZooKeeperServer(zk);
            }
            // Everything is a go, simply start counting the ticks
            // WARNING: I couldn't find any wait statement on a synchronized
            // block that would be notified by this notifyAll() call, so
            // I commented it out
            //synchronized (this) {
            //    notifyAll();
            //}
            // We ping twice a tick, so we only update the tick every other
            // iteration
            boolean tickSkip = true;//一个ticket ping2次
    
            while (true) {
                Thread.sleep(self.tickTime / 2); //睡眠每个ticket周期的一办
                if (!tickSkip) {
                    self.tick.incrementAndGet();//每俩次代表一个ticket
                }
                HashSet<Long> syncedSet = new HashSet<Long>();

                // lock on the followers when we use it.
                syncedSet.add(self.getId());//添加自身的sid

                for (LearnerHandler f : getLearners()) {
                    // Synced set is used to check we have a supporting quorum, so only
                    // PARTICIPANT, not OBSERVER, learners should be used
                    if (f.synced() && f.getLearnerType() == LearnerType.PARTICIPANT) {//保持同步且是参与者
                        syncedSet.add(f.getSid());//添加集合
                    }
                    f.ping();//验证有没有LearnerHandler的proposal超时了没有处理
                }

                // check leader running status
                if (!this.isRunning()) {
                    shutdown("Unexpected internal error");
                    return;
                }

              if (!tickSkip && !self.getQuorumVerifier().containsQuorum(syncedSet)) {////需要验证时，集群验证过半验证失败
                //if (!tickSkip && syncedCount < self.quorumPeers.size() / 2) {
                    // Lost quorum, shutdown
                    shutdown("Not sufficient followers synced, only synced with sids: [ "
                            + getSidSetString(syncedSet) + " ]");
                    // make sure the order is the same!
                    // the leader goes to looking
                    return;
              } 
              tickSkip = !tickSkip;//翻转，进入下半个tickTime
            }
        } finally {
            zk.unregisterJMX(this);
        }
    }

    boolean isShutdown;

    /**
     * Close down all the LearnerHandlers
     */
    void shutdown(String reason) {
        LOG.info("Shutting down");

        if (isShutdown) {
            return;
        }
        
        LOG.info("Shutdown called",
                new Exception("shutdown Leader! reason: " + reason));

        if (cnxAcceptor != null) {
            cnxAcceptor.halt();
        }
        
        // NIO should not accept conenctions
        self.cnxnFactory.setZooKeeperServer(null);
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during close",e);
        }
        // clear all the connections
        self.cnxnFactory.closeAll();
        // shutdown the previous zk
        if (zk != null) {
            zk.shutdown();
        }
        synchronized (learners) {
            for (Iterator<LearnerHandler> it = learners.iterator(); it
                    .hasNext();) {
                LearnerHandler f = it.next();
                it.remove();
                f.shutdown();
            }
        }
        isShutdown = true;
    }

    /**
     * Keep a count of acks that are received by the leader for a particular
     * proposal
     * 针对提议回复ACK的处理逻辑，如果过半验证了就通知所有Learner
     * @param zxid
     *                the zxid of the proposal sent out
     * @param followerAddr
     */
    synchronized public void processAck(long sid, long zxid, SocketAddress followerAddr) {
        if (LOG.isTraceEnabled()) { //log相关
            LOG.trace("Ack zxid: 0x{}", Long.toHexString(zxid));
            for (Proposal p : outstandingProposals.values()) {
                long packetZxid = p.packet.getZxid();
                LOG.trace("outstanding proposal: 0x{}",
                        Long.toHexString(packetZxid));
            }
            LOG.trace("outstanding proposals all");
        }

        if ((zxid & 0xffffffffL) == 0) {
            /*
             * We no longer process NEWLEADER ack by this method. However,
             * the learner sends ack back to the leader after it gets UPTODATE
             * so we just ignore the message.
             */
            return;
        }
    
        if (outstandingProposals.size() == 0) {//没有处理当中的提议
            if (LOG.isDebugEnabled()) {
                LOG.debug("outstanding is 0");
            }
            return;
        }
        if (lastCommitted >= zxid) {//提议已经处理
            if (LOG.isDebugEnabled()) {
                LOG.debug("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        Long.toHexString(lastCommitted), Long.toHexString(zxid));
            }
            // The proposal has already been committed
            return;
        }
        Proposal p = outstandingProposals.get(zxid);//获取提议
        if (p == null) {
            LOG.warn("Trying to commit future proposal: zxid 0x{} from {}",
                    Long.toHexString(zxid), followerAddr);
            return;
        }
        
        p.ackSet.add(sid);//对应提议的ack集合添加sid记录
        if (LOG.isDebugEnabled()) {
            LOG.debug("Count for zxid: 0x{} is {}",
                    Long.toHexString(zxid), p.ackSet.size());
        }
        if (self.getQuorumVerifier().containsQuorum(p.ackSet)){//过半回复的ack
            if (zxid != lastCommitted+1) {
                LOG.warn("Commiting zxid 0x{} from {} not first!",
                        Long.toHexString(zxid), followerAddr);
                LOG.warn("First is 0x{}", Long.toHexString(lastCommitted + 1));
            }
            outstandingProposals.remove(zxid);//该proposal已经处理完了
            if (p.request != null) {
                toBeApplied.add(p);//即将应用的队列添加提议
            }

            if (p.request == null) {
                LOG.warn("Going to commmit null request for proposal: {}", p);
            }
            commit(zxid);//提交发给所有参与者
            inform(p);//告诉所有的观察者
            zk.commitProcessor.commit(p.request);//leader自己也提交
            if(pendingSyncs.containsKey(zxid)){
                for(LearnerSyncRequest r: pendingSyncs.remove(zxid)) {
                    sendSync(r);//发送同步请求给LearnerSyncRequest记录的server
                }
            }
        }
    }

    /**
     * 该处理器有一个toBeApplied队列，用来存储那些已经被CommitProcessor处理过的可被提交的Proposal。
     * 其会将这些请求交付给FinalRequestProcessor处理器处理，待其处理完后，再将其从toBeApplied队列中移除
     *
     */
    static class ToBeAppliedRequestProcessor implements RequestProcessor {
        private RequestProcessor next;

        private ConcurrentLinkedQueue<Proposal> toBeApplied;//并发队列

        /**
         * This request processor simply maintains the toBeApplied list. For
         * this to work next must be a FinalRequestProcessor and
         * FinalRequestProcessor.processRequest MUST process the request
         * synchronously!
         * 
         * @param next
         *                a reference to the FinalRequestProcessor
         */
        ToBeAppliedRequestProcessor(RequestProcessor next,
                ConcurrentLinkedQueue<Proposal> toBeApplied) {
            if (!(next instanceof FinalRequestProcessor)) { ////下一个必须是FinalRequestProcessor
                throw new RuntimeException(ToBeAppliedRequestProcessor.class
                        .getName()
                        + " must be connected to "
                        + FinalRequestProcessor.class.getName()
                        + " not "
                        + next.getClass().getName());
            }
            this.toBeApplied = toBeApplied;
            this.next = next;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.zookeeper.server.RequestProcessor#processRequest(org.apache.zookeeper.server.Request)
         */
        public void processRequest(Request request) throws RequestProcessorException {
            // request.addRQRec(">tobe");
            next.processRequest(request);


            Proposal p = toBeApplied.peek();
            if (p != null && p.request != null
                    && p.request.zxid == request.zxid) {
                toBeApplied.remove();//进行相关Clear
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.zookeeper.server.RequestProcessor#shutdown()
         */
        public void shutdown() {
            LOG.info("Shutting down");
            next.shutdown();
        }
    }

    /**
     * send a packet to all the followers ready to follow
     * 
     * @param qp
     *                the packet to be sent
     */
    void sendPacket(QuorumPacket qp) {
        synchronized (forwardingFollowers) {
            for (LearnerHandler f : forwardingFollowers) {                
                f.queuePacket(qp);
            }
        }
    }
    
    /**
     * send a packet to all observers     
     */
    void sendObserverPacket(QuorumPacket qp) {        
        for (LearnerHandler f : getObservingLearners()) {
            f.queuePacket(qp);
        }
    }

    long lastCommitted = -1;//最近commit的zxid

    /**
     * Create a commit packet and send it to all the members of the quorum
     * 发送给所有的参与者
     * @param zxid
     */
    public void commit(long zxid) {
        synchronized(this){
            lastCommitted = zxid;
        }
        QuorumPacket qp = new QuorumPacket(Leader.COMMIT, zxid, null, null);
        sendPacket(qp);
    }
    
    /**
     * Create an inform packet and send it to all observers.
     * @param zxid
     * @param proposal
     */
    public void inform(Proposal proposal) {   
        QuorumPacket qp = new QuorumPacket(Leader.INFORM, proposal.request.zxid, 
                                            proposal.packet.getData(), null);
        sendObserverPacket(qp);
    }

    long lastProposed;//最近一次提议的zxid

    
    /**
     * Returns the current epoch of the leader.
     * 
     * @return
     */
    public long getEpoch(){
        return ZxidUtils.getEpochFromZxid(lastProposed);
    }
    
    @SuppressWarnings("serial")
    //xid回滚异常
    public static class XidRolloverException extends Exception {
        public XidRolloverException(String message) {
            super(message);
        }
    }

    /**
     * create a proposal and send it out to all the members
     * 
     * @param request
     * @return the proposal that is queued to send to all the members
     *
     */
    public Proposal propose(Request request) throws XidRolloverException {//根据reques的提议发送给所有的参与者
        /**
         * Address the rollover issue. All lower 32bits set indicate a new leader
         * election. Force a re-election instead. See ZOOKEEPER-1277
         */
        if ((request.zxid & 0xffffffffL) == 0xffffffffL) {
            String msg =
                    "zxid lower 32 bits have rolled over, forcing re-election, and therefore new epoch start";
            shutdown(msg);
            throw new XidRolloverException(msg);
        }
        //
        byte[] data = SerializeUtils.serializeRequest(request);
        proposalStats.setLastProposalSize(data.length);
        QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, data, null);//生成提议packte
        
        Proposal p = new Proposal(); //new 一个提议
        p.packet = pp;
        p.request = request;
        synchronized (this) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Proposing:: " + request);
            }

            lastProposed = p.packet.getZxid();///更新最近一次提议的zxid
            outstandingProposals.put(lastProposed, p);//添加map
            sendPacket(pp); //提议发给所有参与者
        }
        return p;
    }
            
    /**
     * Process sync requests
     * 
     * @param r the request
     *
     * 处理同步请求
     */
    synchronized public void processSync(LearnerSyncRequest r){
        if(outstandingProposals.isEmpty()){//没有正在处理的提议
            sendSync(r); //发送同步请求给LearnerSyncRequest记录的server
        } else {
            List<LearnerSyncRequest> l = pendingSyncs.get(lastProposed); ////把当时最新的lastProposed记录下来
            if (l == null) {
                l = new ArrayList<LearnerSyncRequest>();
            }
            l.add(r); //lastProposed对应的记录添加进当前的request,这个列表的同步都是到lastProposed这个位置
            pendingSyncs.put(lastProposed, l);
        }
    }
        
    /**
     * Sends a sync message to the appropriate server
     * 发送Sync请求给合适的server(LearnerSyncRequest记录的)
     * @param f
     * @param r
     */
            
    public void sendSync(LearnerSyncRequest r){
        QuorumPacket qp = new QuorumPacket(Leader.SYNC, 0, null, null);
        r.fh.queuePacket(qp);
    }
                
    /**
     * lets the leader know that a follower is capable of following and is done
     * syncing
     * 就是learner同步的时候，同步方式是和leader的commitLog相关的，在此期间，记录在内存中的提议，以及即将生效的提议也要告诉给learner
     * @param handler handler of the follower
     * @return last proposed zxid
     *
     */
    synchronized public long startForwarding(LearnerHandler handler,
            long lastSeenZxid) { ////让leader知道Follower在进行同步,另外看zk当前有没有新的提议,或者同步的信息发送过去的
        // Queue up any outstanding requests enabling the receipt of
        // new requests
        if (lastProposed > lastSeenZxid) {//自己的zxid比发送给learner的zxid大
            for (Proposal p : toBeApplied) {
                if (p.packet.getZxid() <= lastSeenZxid) {//即将生效的zxid，对应learner已经有记录了
                    continue;
                }
                handler.queuePacket(p.packet);//发送至队列
                // Since the proposal has been committed we need to send the
                // commit message also
                QuorumPacket qp = new QuorumPacket(Leader.COMMIT, p.packet
                        .getZxid(), null, null); //添加commit
                handler.queuePacket(qp); //发送至
            }
            // Only participant need to get outstanding proposals
            if (handler.getLearnerType() == LearnerType.PARTICIPANT) {//如果是参与者，顺便把提议也发送过去
                List<Long>zxids = new ArrayList<Long>(outstandingProposals.keySet());
                Collections.sort(zxids);
                for (Long zxid: zxids) {
                    if (zxid <= lastSeenZxid) {
                        continue;
                    }
                    handler.queuePacket(outstandingProposals.get(zxid).packet);//发送至队列
                }
            }
        }
        if (handler.getLearnerType() == LearnerType.PARTICIPANT) {//如果是参与者
            addForwardingFollower(handler);//添加至参与者集合
        } else {
            addObserverLearnerHandler(handler);//观察者 添加观察者集合
        }
                
        return lastProposed;
    }
    // VisibleForTesting
    protected Set<Long> connectingFollowers = new HashSet<Long>();//连接上leader的sid集合


    /**
     * 被Leader和LearnerHandler调用，即获取集群最大的lastAcceptedEpoch，+1用于设置新的acceptedEpoch
     * @param sid
     * @param lastAcceptedEpoch
     * @return
     * @throws InterruptedException
     * @throws IOException
     */
    public long getEpochToPropose(long sid, long lastAcceptedEpoch) throws InterruptedException, IOException {
        synchronized(connectingFollowers) {//连接的机器
            if (!waitingForNewEpoch) {//如果还在等待new epoch
                return epoch;
            }
            if (lastAcceptedEpoch >= epoch) {//选出自己的的epoch和lastAcceptedEpoch之间比较大的值 + 1 为新的epoch
                epoch = lastAcceptedEpoch+1;
            }
            if (isParticipant(sid)) {//是否是参与者
                connectingFollowers.add(sid);
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            if (connectingFollowers.contains(self.getId()) && 
                                            verifier.containsQuorum(connectingFollowers)) {//如果自己连接上了,并且已经有过半的机器连接上
                waitingForNewEpoch = false;//不用等了
                self.setAcceptedEpoch(epoch);//设置opoch
                connectingFollowers.notifyAll();//唤醒
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                while(waitingForNewEpoch && cur < end) {
                    connectingFollowers.wait(end - cur); //如果已经有机器连接上leader了，那么最多等待一段时间直到其他机器通过 过半验证
                    cur = Time.currentElapsedTime();
                }
                if (waitingForNewEpoch) {//time-out
                    throw new InterruptedException("Timeout while waiting for epoch from quorum");        
                }
            }
            return epoch;//返回epoch
        }
    }
    // VisibleForTesting
    protected Set<Long> electingFollowers = new HashSet<Long>();//针对LEADERINFO回复ACKEPOCH的集合

    // VisibleForTesting
    protected boolean electionFinished = false;//是否过半机器注册成功

    /**
     * 验证leader的StateSummary是最新的
     * 等待过半机器(Learner和leader)针对Leader发出的LEADERINFO回复ACKEPOCH
     * @param id
     * @param ss
     * @throws IOException
     * @throws InterruptedException
     */
    public void waitForEpochAck(long id, StateSummary ss) throws IOException, InterruptedException {
        synchronized(electingFollowers) {
            if (electionFinished) {//如果过半已经回复了
                return;
            }
            if (ss.getCurrentEpoch() != -1) {
                if (ss.isMoreRecentThan(leaderStateSummary)) { //如果有currentEpoch，zxid比Leader自己的还要新
                    throw new IOException("Follower is ahead of the leader, leader summary: " 
                                                    + leaderStateSummary.getCurrentEpoch()
                                                    + " (current epoch), "
                                                    + leaderStateSummary.getLastZxid()
                                                    + " (last zxid)");
                }
                if (isParticipant(id)) {//判断是不是参与者
                    electingFollowers.add(id);//添加进去
                }
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            if (electingFollowers.contains(self.getId()) && verifier.containsQuorum(electingFollowers)) {//过半验证通过
                electionFinished = true;
                electingFollowers.notifyAll();
            } else {                
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                while(!electionFinished && cur < end) {
                    electingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!electionFinished) {
                    throw new InterruptedException("Timeout while waiting for epoch to be acked by quorum");
                }
            }
        }
    }

    /**
     * Return a list of sid in set as string  
     */
    private String getSidSetString(Set<Long> sidSet) {
        StringBuilder sids = new StringBuilder();
        Iterator<Long> iter = sidSet.iterator();
        while (iter.hasNext()) {
            sids.append(iter.next());
            if (!iter.hasNext()) {
              break;
            }
            sids.append(",");
        }
        return sids.toString();
    }

    /**
     * Start up Leader ZooKeeper server and initialize zxid to the new epoch
     * 启动zk server
     */
    private synchronized void startZkServer() {
        // Update lastCommitted and Db's zxid to a value representing the new epoch
        lastCommitted = zk.getZxid();
        LOG.info("Have quorum of supporters, sids: [ "
                + getSidSetString(newLeaderProposal.ackSet)
                + " ]; starting up and setting last processed zxid: 0x{}",
                Long.toHexString(zk.getZxid()));
        zk.startup();
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(getEpoch());

        zk.getZKDatabase().setlastProcessedZxid(zk.getZxid());
    }

    /**
     * Process NEWLEADER ack of a given sid and wait until the leader receives
     * sufficient acks.
     * 等到有过半的参与者针对Leader发出的NEWLEADER返回ACK
     * @param sid
     * @throws InterruptedException
     */
    public void waitForNewLeaderAck(long sid, long zxid)
            throws InterruptedException {

        synchronized (newLeaderProposal.ackSet) {

            if (quorumFormed) {//过半已经同步
                return;
            }

            long currentZxid = newLeaderProposal.packet.getZxid();//获取zxid
            if (zxid != currentZxid) {
                LOG.error("NEWLEADER ACK from sid: " + sid
                        + " is from a different epoch - current 0x"
                        + Long.toHexString(currentZxid) + " receieved 0x"
                        + Long.toHexString(zxid));
                return;
            }

            if (isParticipant(sid)) {
                newLeaderProposal.ackSet.add(sid);//只有参与者返回的ack才算数
            }

            if (self.getQuorumVerifier().containsQuorum(
                    newLeaderProposal.ackSet)) {
                quorumFormed = true;
                newLeaderProposal.ackSet.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                while (!quorumFormed && cur < end) {
                    newLeaderProposal.ackSet.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!quorumFormed) {
                    throw new InterruptedException(
                            "Timeout while waiting for NEWLEADER to be acked by quorum");
                }
            }
        }
    }

    /**
     * Get string representation of a given packet type
     * @param packetType
     * @return string representing the packet type
     */
    public static String getPacketType(int packetType) {
        switch (packetType) {
        case DIFF:
            return "DIFF";
        case TRUNC:
            return "TRUNC";
        case SNAP:
            return "SNAP";
        case OBSERVERINFO:
            return "OBSERVERINFO";
        case NEWLEADER:
            return "NEWLEADER";
        case FOLLOWERINFO:
            return "FOLLOWERINFO";
        case UPTODATE:
            return "UPTODATE";
        case LEADERINFO:
            return "LEADERINFO";
        case ACKEPOCH:
            return "ACKEPOCH";
        case REQUEST:
            return "REQUEST";
        case PROPOSAL:
            return "PROPOSAL";
        case ACK:
            return "ACK";
        case COMMIT:
            return "COMMIT";
        case PING:
            return "PING";
        case REVALIDATE:
            return "REVALIDATE";
        case SYNC:
            return "SYNC";
        case INFORM:
            return "INFORM";
        default:
            return "UNKNOWN";
        }
    }

    private boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }

    private boolean isParticipant(long sid) {
        return self.getVotingView().containsKey(sid);
    }
}
