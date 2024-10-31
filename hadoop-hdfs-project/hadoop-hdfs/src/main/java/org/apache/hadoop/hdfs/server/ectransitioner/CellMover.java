package org.apache.hadoop.hdfs.server.ectransitioner;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.common.sps.BlockDispatcher;
import org.apache.hadoop.hdfs.server.namenode.sps.BlockMoveTaskHandler;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CellMover handles the logic and mechanics behind sending a cell's worth of data
 * between datanodes. It encapsulates the cell's byte range in the striped block in a new block
 * and sends that new wrapped block over the network to the target datanode.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CellMover implements BlockMoveTaskHandler {

    private static final int DFS_MOVER_MOVERTHREADS_DEFAULT_OVERRIDE = 100;

    private static final Logger LOG = LoggerFactory.getLogger(CellMover.class);

    private final NameNodeConnector nnc;
    private final SaslDataTransferClient saslClient;
    private final ExecutorService moveExecutor;
    private final BlockDispatcher dispatcher;

    public CellMover(NameNodeConnector nnc, Configuration conf) {
        this.nnc = nnc;

        int maxThreads = conf.getInt(DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY,
                DFS_MOVER_MOVERTHREADS_DEFAULT_OVERRIDE);
        moveExecutor = initBlockMover(maxThreads);

        this.saslClient = new SaslDataTransferClient(conf,
                DataTransferSaslUtil.getSaslPropertiesResolver(conf),
                TrustedChannelResolver.getInstance(conf),
                nnc.getFallbackToSimpleAuth());

        boolean connectToDnViaHostname = conf.getBoolean(
                HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME,
                HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
        int ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(conf);
        dispatcher = new BlockDispatcher(HdfsConstants.READ_TIMEOUT,
                ioFileBufferSize, connectToDnViaHostname);
    }

    private ThreadPoolExecutor initBlockMover(int maxThreads) {
        ThreadPoolExecutor moverThreadPool = new ThreadPoolExecutor(1, maxThreads, 60,
            TimeUnit.SECONDS, new SynchronousQueue<>(),
            new Daemon.DaemonFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = super.newThread(r);
                    t.setName("BlockMoverTask-" + threadIndex.getAndIncrement());
                    return t;
                }
            }, new ThreadPoolExecutor.CallerRunsPolicy() {
            @Override
            public void rejectedExecution(Runnable runnable,
                                          ThreadPoolExecutor e) {
                LOG.info("Execution for block movement to satisfy storage policy"
                        + " got rejected, Executing in current thread");
                // will run in the current thread.
                super.rejectedExecution(runnable, e);
            }
        });
        moverThreadPool.allowCoreThreadTimeOut(true);
        return moverThreadPool;
    }

    /**
     * Migrate a stripe from the blockGroup to the new datanodes, truncating from
     * the current blocks and creating a new block on the new datanodes
     * @param blockGroup
     * @param newDataNodes
     * @param ecPolicy
     * @param configuration
     */
    public void migrateCell(LocatedStripedBlock blockGroup,
                            List<DatanodeInfo> newDataNodes,
                            ErasureCodingPolicy ecPolicy,
                            Configuration configuration) {
        // striped block group can be split up into multiple internal blocks
        byte blockIndex = 0; // TODO: arbitrary, need to move all of them eventually
        byte locationIndex = 0;
        for (byte i = 0; i < blockGroup.getBlockIndices().length; i++) {
            // find where the block index matches which block you're trying to read
            if (blockGroup.getBlockIndices()[i] == blockIndex) {
                locationIndex = i;
            }
        }
        int cellSize = ecPolicy.getCellSize();
        DatanodeInfo source = blockGroup.getLocations()[locationIndex]; // choose the first
        DatanodeInfo target = newDataNodes.get(0); // choose just one datanode to move to

        LOG.info("Executing cell migration: ");
        LOG.info("Block index = {}, stripe index = {}", blockIndex, locationIndex);
        LOG.info("Source datanode = {}", source.toString());
        LOG.info("Target datanode = {}", target.toString());

        // construct new blocks
        LocatedBlock internalLocatedBlock = StripedBlockUtil.constructInternalBlock(
                blockGroup, 0, cellSize,
                ecPolicy.getNumDataUnits(), 0);
        ExtendedBlock internalExtendedBlock = internalLocatedBlock.getBlock();
        internalExtendedBlock.setNumBytes(cellSize);

        // get last cell start position
        long length = StripedBlockUtil.getInternalBlockLength(
                blockGroup.getBlock().getNumBytes(), ecPolicy, blockIndex); // might be locationIndex, not sure
        long offset = length - cellSize;
        LOG.info("Internal block length = {} and corresponding offset = {}", length, offset);

        BlockMovingInfo movingInfo = new BlockMovingInfo(
                internalExtendedBlock.getLocalBlock(),
                source, target, StorageType.DEFAULT,
                StorageType.DEFAULT, offset);

        submitMoveTask(movingInfo);

        try {
            // wait a bit to let move task complete
            Thread.sleep(1000);
	        LOG.info("Source address = {}", source.getIpcAddr(false));
            ClientDatanodeProtocol sourceDatanode = getDataNodeProxy(source.getIpcAddr(false), configuration);

            // tell source datanode to truncate its block by one cell's worth
            // currently using this RPC to test block id modification so pointing it at the target datanode
            // instead of the source
            // try it on source too just to see
            sourceDatanode.triggerCellMovement(internalExtendedBlock, 1024, 1024);

            // wait a bit to let truncate complete
            Thread.sleep(1000);
            LOG.info("Done truncating the original block");
        } catch (IOException e) {
            LOG.error("Unable to make a connection to the datanode directly", e);
        } catch (InterruptedException e) {
            LOG.error("Failed to sleep", e);
        }
    }

    @Override
    public void submitMoveTask(BlockMovingInfo movingInfo) {
        if (moveExecutor == null) {
            LOG.warn("No mover threads available, passing on the move operation");
            return;
        }

        moveExecutor.execute(() -> moveCell(movingInfo));
    }

    private void moveCell(BlockMovingInfo movingInfo) {
        ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(), movingInfo.getBlock());
        final KeyManager km = nnc.getKeyManager();
        Token<BlockTokenIdentifier> accessToken;
        try {
            accessToken = km.getAccessToken(eb,
                    new StorageType[]{movingInfo.getTargetStorageType()},
                    new String[0]);
        } catch (IOException e) {
            LOG.warn("Unable to retrieve the access token to move block");
            return;
        }
        dispatcher.moveBlock(movingInfo, saslClient, eb, new Socket(), km, accessToken);
    }

    /**
     * Copied from DFSAdmin.java. -- Creates a connection to dataNode.
     *
     * @param datanode - dataNode.
     * @return ClientDataNodeProtocol
     */
    public ClientDatanodeProtocol getDataNodeProxy(String datanode, Configuration conf)
            throws IOException {
        InetSocketAddress datanodeAddr = NetUtils.createSocketAddr(datanode);

        // For datanode proxy the server principal should be DN's one.
        conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
                conf.get(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, ""));

        // Create the client
        ClientDatanodeProtocol dnProtocol =
                DFSUtilClient.createClientDatanodeProtocolProxy(datanodeAddr, getUGI(),
                        conf, NetUtils.getSocketFactory(conf, ClientDatanodeProtocol.class));
        return dnProtocol;
    }

    /**
     * Returns UGI.
     *
     * @return UserGroupInformation.
     */
    private static UserGroupInformation getUGI()
            throws IOException {
        return UserGroupInformation.getCurrentUser();
    }

}
