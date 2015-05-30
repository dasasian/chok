/**
 * Copyright (C) 2014 Dasasian (damith@dasasian.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dasasian.chok.protocol;

import com.dasasian.chok.master.Master;
import com.dasasian.chok.node.Node;
import com.dasasian.chok.node.monitor.MetricsRecord;
import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.master.MasterOperation;
import com.dasasian.chok.operation.node.NodeOperation;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.protocol.metadata.IndexMetaData.Shard;
import com.dasasian.chok.protocol.metadata.MasterMetaData;
import com.dasasian.chok.protocol.metadata.NodeMetaData;
import com.dasasian.chok.protocol.metadata.Version;
import com.dasasian.chok.util.*;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.util.ZkPathUtil;
import org.I0Itec.zkclient.util.ZkPathUtil.PathFilter;
import org.slf4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Abstracts the interaction between master and nodes via zookeeper files and
 * folders.
 * <br>
 * For inspecting and understanding the zookeeper structure see
 * {@link #explainStructure()} and {@link #showStructure(boolean)}.
 */
public class InteractionProtocol {

    protected final static Logger LOG = LoggerFactory.getLogger(InteractionProtocol.class);

    public static final String DISABLE_INDEX_AUTO_REPAIR_FLAG = "disable-index-auto-repair";

    protected final ZkClient zkClient;
    protected final ZkConfiguration zkConf;
    protected volatile boolean connected = true;
    // we govern the various listener and ephemerals to remove burden from
    // listener-users to unregister/delete them
    protected One2ManyListMap<ConnectedComponent, ListenerAdapter> zkListenerByComponent = new One2ManyListMap<>();
    private SetMultimap<ConnectedComponent, String> zkEphemeralPublishesByComponent = HashMultimap.create();

    private IZkStateListener stateListener = new IZkStateListener() {
        @Override
        public void handleStateChanged(KeeperState state) throws Exception {
            Set<ConnectedComponent> components = new HashSet<>(zkListenerByComponent.keySet());
            switch (state) {
                case Disconnected:
                case Expired:
                    if (connected) { // disconnected & expired can come after each other
                        for (ConnectedComponent component : components) {
                            component.disconnect();
                        }
                        connected = false;
                    }
                    break;
                case SyncConnected:
                    connected = true;
                    for (ConnectedComponent chokComponent : components) {
                        chokComponent.reconnect();
                    }
                    break;
                default:
                    throw new IllegalStateException("state " + state + " not handled");
            }
        }

        @Override
        public void handleNewSession() throws Exception {
            // should be covered by handleStateChanged()
        }

        @Override
        public void handleSessionEstablishmentError(Throwable error) throws Exception {
            throw new Exception("count not establish session", error);

        }
    };

    public InteractionProtocol(ZkClient zkClient, ZkConfiguration zkConfiguration) {
        this.zkClient = zkClient;
        zkConf = zkConfiguration;
        LOG.debug("Using ZK root path: " + zkConf.getRootPath());
        new DefaultNameSpaceImpl(zkConf).createDefaultNameSpace(this.zkClient);
    }

    public void registerComponent(ConnectedComponent connectedComponent) {
        if (zkListenerByComponent.size() == 0) {
            zkClient.subscribeStateChanges(stateListener);
        }
        zkListenerByComponent.add(connectedComponent);
    }

    public void unregisterComponent(ConnectedComponent component) {
        List<ListenerAdapter> listeners = zkListenerByComponent.removeKey(component);
        for (ListenerAdapter listener : listeners) {
            if (listener instanceof IZkChildListener) {
                zkClient.unsubscribeChildChanges(listener.getPath(), (IZkChildListener) listener);
            } else if (listener instanceof IZkDataListener) {
                zkClient.unsubscribeDataChanges(listener.getPath(), (IZkDataListener) listener);
            } else {
                throw new IllegalStateException("could not handle lister of type " + listener.getClass().getName());
            }
        }
        // we deleting the ephemeral's since this is the fastest and the safest

        // way, but if this does not work, it shouldn't be too bad
        Collection<String> zkPublishes = zkEphemeralPublishesByComponent.removeAll(component);
        for (String zkPath : zkPublishes) {
            zkClient.delete(zkPath);
        }

        if (zkListenerByComponent.size() == 0) {
            zkClient.unsubscribeStateChanges(stateListener);
        }
        LOG.info("unregistering component " + component + ": " + zkListenerByComponent);
    }

    public void disconnect() {
        if (zkListenerByComponent.size() > 0) {
            LOG.warn("following components still connected:" + zkListenerByComponent.keySet());
        }
        if (zkEphemeralPublishesByComponent.size() > 0) {
            LOG.warn("following ephemeral still exists:" + zkEphemeralPublishesByComponent.keySet());
        }
        connected = false;
        zkClient.close();
    }

    public int getRegisteredComponentCount() {
        return zkListenerByComponent.size();
    }

    public int getRegisteredListenerCount() {
        int count = 0;
        Collection<List<ListenerAdapter>> values = zkListenerByComponent.asMap().values();
        for (List<ListenerAdapter> list : values) {
            count += list.size();
        }
        return count;
    }

    public List<String> registerChildListener(ConnectedComponent component, PathDef pathDef, IAddRemoveListener listener) {
        return registerAddRemoveListener(component, listener, zkConf.getPath(pathDef));
    }

    public List<String> registerChildListener(ConnectedComponent component, PathDef pathDef, String childName, IAddRemoveListener listener) {
        return registerAddRemoveListener(component, listener, zkConf.getPath(pathDef, childName));
    }

    public void unregisterChildListener(ConnectedComponent component, PathDef pathDef) {
        unregisterAddRemoveListener(component, zkConf.getPath(pathDef));
    }

    public void unregisterChildListener(ConnectedComponent component, PathDef pathDef, String childName) {
        unregisterAddRemoveListener(component, zkConf.getPath(pathDef, childName));
    }

    public void registerDataListener(ConnectedComponent component, PathDef pathDef, String childName, IZkDataListener listener) {
        registerDataListener(component, listener, zkConf.getPath(pathDef, childName));
    }

    public void unregisterDataChanges(ConnectedComponent component, PathDef pathDef, String childName) {
        unregisterDataListener(component, zkConf.getPath(pathDef, childName));
    }

    public void unregisterDataChanges(ConnectedComponent component, String dataPath) {
        unregisterDataListener(component, dataPath);
    }

    private void registerDataListener(ConnectedComponent component, IZkDataListener dataListener, String zkPath) {
        synchronized (component) {
            ZkDataListenerAdapter listenerAdapter = new ZkDataListenerAdapter(dataListener, zkPath);
            zkClient.subscribeDataChanges(zkPath, listenerAdapter);
            zkListenerByComponent.add(component, listenerAdapter);
        }
    }

    private void unregisterDataListener(ConnectedComponent component, String zkPath) {
        synchronized (component) {
            ZkDataListenerAdapter listenerAdapter = getComponentListener(component, ZkDataListenerAdapter.class, zkPath);
            zkClient.unsubscribeDataChanges(zkPath, listenerAdapter);
            zkListenerByComponent.removeValue(component, listenerAdapter);
        }
    }

    private List<String> registerAddRemoveListener(final ConnectedComponent component, final IAddRemoveListener listener, String zkPath) {
        synchronized (component) {
            AddRemoveListenerAdapter listenerAdapter = new AddRemoveListenerAdapter(zkPath, listener);
            synchronized (listenerAdapter) {
                List<String> childs = zkClient.subscribeChildChanges(zkPath, listenerAdapter);
                listenerAdapter.setCachedChilds(childs);
            }
            zkListenerByComponent.add(component, listenerAdapter);
            return listenerAdapter.getCachedChilds();
        }
    }

    private void unregisterAddRemoveListener(final ConnectedComponent component, String zkPath) {
        synchronized (component) {
            AddRemoveListenerAdapter listenerAdapter = getComponentListener(component, AddRemoveListenerAdapter.class, zkPath);
            zkClient.unsubscribeChildChanges(zkPath, listenerAdapter);
            zkListenerByComponent.removeValue(component, listenerAdapter);
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends ListenerAdapter> T getComponentListener(final ConnectedComponent component, Class<T> listenerClass, String zkPath) {
        for (ListenerAdapter pathListener : zkListenerByComponent.getValues(component)) {
            if (listenerClass.isAssignableFrom(pathListener.getClass()) && pathListener.getPath().equals(zkPath)) {
                return (T) pathListener;
            }
        }
        throw new IllegalStateException("no listener adapter for component " + component + " and path " + zkPath + " found: " + zkListenerByComponent);
    }

    public List<String> getKnownNodes() {
        return zkClient.getChildren(zkConf.getPath(PathDef.NODES_METADATA));
    }

    public List<String> getLiveNodes() {
        return zkClient.getChildren(zkConf.getPath(PathDef.NODES_LIVE));
    }

    public List<String> getIndices() {
        return zkClient.getChildren(zkConf.getPath(PathDef.INDICES_METADATA));
    }

    public MasterMetaData getMasterMD() {
        return (MasterMetaData) readZkData(zkConf.getPath(PathDef.MASTER));
    }

    public NodeMetaData getNodeMD(String node) {
        return (NodeMetaData) readZkData(zkConf.getPath(PathDef.NODES_METADATA, node));
    }

    private Serializable readZkData(String zkPath) {
        Serializable data = null;
        if (zkClient.exists(zkPath)) {
            data = zkClient.readData(zkPath);
        }
        return data;
    }

    public Collection<String> getNodeShards(String nodeName) {
        Set<String> shards = new HashSet<>();
        List<String> shardNames = zkClient.getChildren(zkConf.getPath(PathDef.SHARD_TO_NODES));
        for (String shardName : shardNames) {
            List<String> nodeNames = zkClient.getChildren(zkConf.getPath(PathDef.SHARD_TO_NODES, shardName));
            if (nodeNames.contains(nodeName)) {
                shards.add(shardName);
            }
        }
        return shards;
    }

    public int getShardReplication(String shard) {
        return zkClient.countChildren(zkConf.getPath(PathDef.SHARD_TO_NODES, shard));
    }

    public long getShardAnnounceTime(String node, String shard) {
        return zkClient.getCreationTime(zkConf.getPath(PathDef.SHARD_TO_NODES, shard, node));
    }

    public Map<String, List<String>> getShard2NodesMap(Collection<String> shardNames) {
        final Map<String, List<String>> shard2NodeNames = new HashMap<>();
        for (final String shard : shardNames) {
            final String shard2NodeRootPath = zkConf.getPath(PathDef.SHARD_TO_NODES, shard);
            if (zkClient.exists(shard2NodeRootPath)) {
                shard2NodeNames.put(shard, zkClient.getChildren(shard2NodeRootPath));
            } else {
                shard2NodeNames.put(shard, Collections.<String>emptyList());
            }
        }
        return shard2NodeNames;
    }

    public List<String> getShardNodes(String shard) {
        final String shard2NodeRootPath = zkConf.getPath(PathDef.SHARD_TO_NODES, shard);
        if (!zkClient.exists(shard2NodeRootPath)) {
            return Collections.emptyList();
        }
        return zkClient.getChildren(shard2NodeRootPath);
    }

    public List<String> getShard2NodeShards() {
        return zkClient.getChildren(zkConf.getPath(PathDef.SHARD_TO_NODES));
    }

    public ReplicationReport getReplicationReport(IndexMetaData indexMD) {
        int desiredReplicationCount = indexMD.getReplicationLevel();
        int minimalShardReplicationCount = indexMD.getReplicationLevel();
        int maximaShardReplicationCount = 0;

        Map<String, Integer> replicationCountByShardMap = new HashMap<>();
        final Set<Shard> shards = indexMD.getShards();
        for (final Shard shard : shards) {
            final int servingNodesCount = getShardNodes(shard.getName()).size();
            replicationCountByShardMap.put(shard.getName(), servingNodesCount);
            if (servingNodesCount < minimalShardReplicationCount) {
                minimalShardReplicationCount = servingNodesCount;
            }
            if (servingNodesCount > maximaShardReplicationCount) {
                maximaShardReplicationCount = servingNodesCount;
            }
        }
        return new ReplicationReport(replicationCountByShardMap, desiredReplicationCount, minimalShardReplicationCount, maximaShardReplicationCount);
    }

    public MasterQueue publishMaster(final Master master) {
        String masterName = master.getMasterName();
        String zkMasterPath = zkConf.getPath(PathDef.MASTER);
        cleanupOldMasterData(masterName, zkMasterPath);

        boolean isMaster;
        try {
            createEphemeral(master, zkMasterPath, new MasterMetaData(masterName, System.currentTimeMillis()));
            isMaster = true;
            LOG.info(masterName + " started as master");
        } catch (ZkNodeExistsException e) {
            registerDataListener(master, new IZkDataListener() {
                @Override
                public void handleDataDeleted(final String dataPath) throws ChokException {
                    master.handleMasterDisappearedEvent();
                }

                @Override
                public void handleDataChange(String dataPath, Object data) throws Exception {
                    // do nothing
                }
            }, zkMasterPath);
            isMaster = false;
            LOG.info(masterName + " started as secondary master");
        }

        MasterQueue queue = null;
        if (isMaster) {
            LOG.info("master '" + master.getMasterName() + "' published");
            String queuePath = zkConf.getPath(PathDef.MASTER_QUEUE);
            if (!zkClient.exists(queuePath)) {
                zkClient.createPersistent(queuePath);
            }
            queue = new MasterQueue(zkClient, queuePath);
        } else {
            LOG.info("secondary master '" + master.getMasterName() + "' registered");
        }

        return queue;
    }

    private void cleanupOldMasterData(final String masterName, String zkMasterPath) {
        if (zkClient.exists(zkMasterPath)) {
            final MasterMetaData existingMaster = zkClient.readData(zkMasterPath);
            if (existingMaster.getMasterName().equals(masterName)) {
                LOG.warn("detected old master entry pointing to this host - deleting it..");
                zkClient.delete(zkMasterPath);
            }
        }
    }

    public NodeQueue publishNode(Node node, NodeMetaData nodeMetaData) {
        LOG.info("publishing node '" + node.getName() + "' ...");
        final String nodePath = zkConf.getPath(PathDef.NODES_LIVE, node.getName());
        final String nodeMetadataPath = zkConf.getPath(PathDef.NODES_METADATA, node.getName());

        // write or update metadata
        if (zkClient.exists(nodeMetadataPath)) {
            zkClient.writeData(nodeMetadataPath, nodeMetaData);
        } else {
            zkClient.createPersistent(nodeMetadataPath, nodeMetaData);
        }

        // create queue for incoming node operations
        String queuePath = zkConf.getPath(PathDef.NODE_QUEUE, node.getName());
        if (zkClient.exists(queuePath)) {
            zkClient.deleteRecursive(queuePath);
        }

        NodeQueue nodeQueue = new NodeQueue(zkClient, queuePath);

        // mark the node as connected
        if (zkClient.exists(nodePath)) {
            LOG.warn("Old node ephemeral '" + nodePath + "' detected, deleting it...");
            zkClient.delete(nodePath);
        }
        createEphemeral(node, nodePath, null);
        return nodeQueue;
    }

    public void publishIndex(IndexMetaData indexMD) {
        zkClient.createPersistent(zkConf.getPath(PathDef.INDICES_METADATA, indexMD.getName()), indexMD);
    }

    public void unpublishIndex(String indexName) {
        IndexMetaData indexMD = getIndexMD(indexName);
        zkClient.delete(zkConf.getPath(PathDef.INDICES_METADATA, indexName));
        for (Shard shard : indexMD.getShards()) {
            zkClient.deleteRecursive(zkConf.getPath(PathDef.SHARD_TO_NODES, shard.getName()));
        }
    }

    public IndexMetaData getIndexMD(String index) {
        return (IndexMetaData) readZkData(zkConf.getPath(PathDef.INDICES_METADATA, index));
    }

    public void updateIndexMD(IndexMetaData indexMD) {
        zkClient.writeData(zkConf.getPath(PathDef.INDICES_METADATA, indexMD.getName()), indexMD);
    }

    public void publishShard(Node node, String shardName) {
        // announce that this node serves this shard now...
        final String shard2NodePath = zkConf.getPath(PathDef.SHARD_TO_NODES, shardName, node.getName());
        if (zkClient.exists(shard2NodePath)) {
            LOG.warn("detected old shard-to-node entry - deleting it..");
            zkClient.delete(shard2NodePath);
        }
        zkClient.createPersistent(zkConf.getPath(PathDef.SHARD_TO_NODES, shardName), true);
        createEphemeral(node, shard2NodePath, null);
    }

    public void unpublishShard(Node node, String shard) {
        String shard2NodePath = zkConf.getPath(PathDef.SHARD_TO_NODES, shard, node.getName());
        if (zkClient.exists(shard2NodePath)) {
            zkClient.delete(shard2NodePath);
        }
        zkEphemeralPublishesByComponent.remove(node, shard2NodePath);
    }

    public void setMetric(String nodeName, MetricsRecord metricsRecord) {
        String metricsPath = zkConf.getPath(PathDef.NODE_METRICS, nodeName);
        try {
            zkClient.writeData(metricsPath, metricsRecord);
        } catch (ZkNoNodeException e) {
            // TODO put in ephemeral map ?
            zkClient.createEphemeral(metricsPath, new MetricsRecord(nodeName));
        } catch (Exception e) {
            // this only happens if zk is down
            LOG.debug("Can't write to zk", e);
        }
    }

    public MetricsRecord getMetric(String nodeName) {
        return (MetricsRecord) readZkData(zkConf.getPath(PathDef.NODE_METRICS, nodeName));
    }

    private void createEphemeral(ConnectedComponent component, String path, Serializable content) {
        zkClient.createEphemeral(path, content);
        zkEphemeralPublishesByComponent.put(component, path);
    }

    public void addMasterOperation(MasterOperation operation) {
        String queuePath = zkConf.getPath(PathDef.MASTER_QUEUE);
        new MasterQueue(zkClient, queuePath).add(operation);
    }

    public OperationId addNodeOperation(String nodeName, NodeOperation nodeOperation) {
        String elementName = getNodeQueue(nodeName).add(nodeOperation);
        return new OperationId(nodeName, elementName);
    }

    public OperationResult getNodeOperationResult(OperationId operationId, boolean remove) {
        return (OperationResult) getNodeQueue(operationId.getNodeName()).getResult(operationId.getElementName(), remove);
    }

    public boolean isNodeOperationQueued(OperationId operationId) {
        return getNodeQueue(operationId.getNodeName()).containsElement(operationId.getElementName());
    }

    public void registerNodeOperationListener(ConnectedComponent component, OperationId operationId, IZkDataListener dataListener) {
        String elementPath = getNodeQueue(operationId.getNodeName()).getElementPath(operationId.getElementName());
        registerDataListener(component, dataListener, elementPath);
    }

    private NodeQueue getNodeQueue(String nodeName) {
        String queuePath = zkConf.getPath(PathDef.NODE_QUEUE, nodeName);
        return new NodeQueue(zkClient, queuePath);
    }

    public boolean indexExists(String indexName) {
        return zkClient.exists(zkConf.getPath(PathDef.INDICES_METADATA, indexName));
    }

    public void showStructure(final boolean all) {
        final String string;
        if (all) {
            string = ZkPathUtil.toString(zkClient, zkConf.getRootPath(), PathFilter.ALL);
        } else {
            final Set<String> nonViPathes = new HashSet<>();
            for (PathDef pathDef : PathDef.values()) {
                if (!pathDef.isVip()) {
                    nonViPathes.add(zkConf.getPath(pathDef));
                }
            }
            string = ZkPathUtil.toString(zkClient, zkConf.getRootPath(), new PathFilter() {
                @Override
                public boolean showChilds(String path) {
                    return !nonViPathes.contains(path);
                }
            });
        }
        System.out.println(string);
    }

    public void explainStructure() {
        for (PathDef pathDef : PathDef.values()) {
            String zkPath = zkConf.getPath(pathDef);
            System.out.println(StringUtil.fillWithWhiteSpace(zkPath, 40) + "\t" + pathDef.getDescription());
        }
    }

    public Version getVersion() {
        return zkClient.readData(zkConf.getPath(PathDef.VERSION), true);
    }

    public void setVersion(Version version) {
        String zkPath = zkConf.getPath(PathDef.VERSION);
        try {
            zkClient.writeData(zkPath, version);
        } catch (ZkNoNodeException e) {
            zkClient.createPersistent(zkPath, version);
        }
    }

    public void setFlag(String name) {
        zkClient.createEphemeral(zkConf.getPath(PathDef.FLAGS, name));
    }

    public boolean flagExists(String name) {
        return zkClient.exists(zkConf.getPath(PathDef.FLAGS, name));
    }

    public void removeFlag(String name) {
        zkClient.delete(zkConf.getPath(PathDef.FLAGS, name));
    }

    public ZkConfiguration getZkConfiguration() {
        return zkConf;
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public void disableIndexAutoRepair() {
        zkClient.createPersistent(zkConf.getPath(PathDef.FLAGS, DISABLE_INDEX_AUTO_REPAIR_FLAG));
    }

    public void enableIndexAutoRepair() {
        zkClient.delete(zkConf.getPath(PathDef.FLAGS, DISABLE_INDEX_AUTO_REPAIR_FLAG));
    }

    public boolean isIndexAutoRepairEnabled() {
        return !zkClient.exists(zkConf.getPath(PathDef.FLAGS, DISABLE_INDEX_AUTO_REPAIR_FLAG));
    }

    static class ListenerAdapter {
        private final String _path;

        public ListenerAdapter(String path) {
            _path = path;
        }

        public final String getPath() {
            return _path;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + ":" + getPath();
        }

    }

    static class AddRemoveListenerAdapter extends ListenerAdapter implements IZkChildListener {

        private final IAddRemoveListener _listener;
        private List<String> _cachedChilds;

        public AddRemoveListenerAdapter(String path, IAddRemoveListener listener) {
            super(path);
            _listener = listener;
        }

        public List<String> getCachedChilds() {
            return _cachedChilds;
        }

        public void setCachedChilds(List<String> cachedChilds) {
            _cachedChilds = cachedChilds;
            if (_cachedChilds == null) {
                _cachedChilds = Collections.emptyList();
            }
        }

        @Override
        public synchronized void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            if (currentChilds == null) {
                currentChilds = Collections.emptyList();
            }
            List<String> addedChilds = CollectionUtil.getListOfAdded(_cachedChilds, currentChilds);
            List<String> removedChilds = CollectionUtil.getListOfRemoved(_cachedChilds, currentChilds);
            for (String addedChild : addedChilds) {
                _listener.added(addedChild);
            }
            for (String removedChild : removedChilds) {
                _listener.removed(removedChild);
            }
            _cachedChilds = currentChilds;
        }
    }

    static class ZkDataListenerAdapter extends ListenerAdapter implements IZkDataListener {

        private final IZkDataListener _dataListener;

        public ZkDataListenerAdapter(IZkDataListener dataListener, String path) {
            super(path);
            _dataListener = dataListener;
        }

        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            _dataListener.handleDataChange(dataPath, data);
        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception {
            _dataListener.handleDataDeleted(dataPath);
        }

    }

}
