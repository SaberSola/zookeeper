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

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor simply forwards requests to an AckRequestProcessor and
 * SyncRequestProcessor.
 * 事务投票处理器。Leader服务器事务处理流程的发起者，对于非事务性请求，
 * ProposalRequestProcessor会直接将请求转发到CommitProcessor处理器，不再做任何处理，而对于事务性请求，
 * 除了将请求转发到CommitProcessor外，还会根据请求类型创建对应的Proposal提议，并发送给所有的Follower服务器来发起一次集群内的事务投票。
 * 同时，ProposalRequestProcessor还会将事务请求交付给SyncRequestProcessor进行事务日志的记录。
 */
public class ProposalRequestProcessor implements RequestProcessor {
    private static final Logger LOG =
        LoggerFactory.getLogger(ProposalRequestProcessor.class);

    LeaderZooKeeperServer zks;//leader角色才有这个请求处理器，这里能拿到LeaderZooKeeperServer
    
    RequestProcessor nextProcessor; //下一个处理器

    SyncRequestProcessor syncProcessor; //下一个处理器

    public ProposalRequestProcessor(LeaderZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        this.zks = zks;
        this.nextProcessor = nextProcessor;//一般是CommitProcessor
        AckRequestProcessor ackProcessor = new AckRequestProcessor(zks.getLeader());
        syncProcessor = new SyncRequestProcessor(zks, ackProcessor);//syncProcessor的后续是AckRequestProcessor
    }
    
    /**
     * initialize this processor
     * 启动同步处理器
     */
    public void initialize() {
        syncProcessor.start();
    }
    
    public void processRequest(Request request) throws RequestProcessorException {
        // LOG.warn("Ack>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = " + request.sessionId);
        // request.addRQRec(">prop");
                
        
        /* In the following IF-THEN-ELSE block, we process syncs on the leader. 
         * If the sync is coming from a follower, then the follower
         * handler adds it to syncHandler. Otherwise, if it is a client of
         * the leader that issued the sync command, then syncHandler won't 
         * contain the handler. In this case, we add it to syncHandler, and 
         * call processRequest on the next processor.
         */
        
        if(request instanceof LearnerSyncRequest){//如果是client的同步请求
            zks.getLeader().processSync((LearnerSyncRequest)request);//特殊处理，不臫走调用链的,根据lastProposed记录，processAck函数异步处理时时给对应的LearnerHandler发送Sync的消息
        } else {
                nextProcessor.processRequest(request);//交给CommitProcessor处理
            if (request.hdr != null) {//请求头不为空 说明是事务请求
                // We need to sync and get consensus on any transactions
                try {
                    zks.getLeader().propose(request);//leader发出提议,集群进行投票
                } catch (XidRolloverException e) {
                    throw new RequestProcessorException(e.getMessage(), e);
                }
                syncProcessor.processRequest(request); //事务请求需要syncProcessor进行处理
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
        syncProcessor.shutdown();
    }

}
