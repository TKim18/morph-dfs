package org.apache.hadoop.hdfs.server.datanode.erasurecode.transcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.BlockECTranscodeCommand.BlockECTranscodeInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockECTranscodeCommand;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ErasureTranscoderWorker handles the transcoding work commands.
 * These commands would be issued from Namenode as part of Datanode's
 * heart beat response. BPOfferService delegates the work to this class
 * for handling EC commands.
 */
@InterfaceAudience.Private
public class ErasureTranscodingWorker {

  private static final Logger LOG = DataNode.LOG;

  private final DataNode datanode;
  private final Configuration conf;

  private ThreadPoolExecutor stripedTranscoderPool;
  private ThreadPoolExecutor stripedReadPool;

  public ErasureTranscodingWorker(Configuration conf, DataNode datanode) {
    this.datanode = datanode;
    this.conf = conf;
    initializeStripedReadThreadPool();
    initializedTranscoderThreadPool(conf.getInt(
        DFSConfigKeys.DFS_DN_EC_TRANSCODER_THREADS_KEY,
        DFSConfigKeys.DFS_DN_EC_TRANSCODER_THREADS_DEFAULT));
  }

  private void initializeStripedReadThreadPool() {
    // Essentially, this is a cachedThreadPool.
    stripedReadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
        60, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new Daemon.DaemonFactory() {
          private final AtomicInteger threadIndex = new AtomicInteger(0);

          @Override
          public Thread newThread(Runnable r) {
            Thread t = super.newThread(r);
            t.setName("stripedRead-" + threadIndex.getAndIncrement());
            return t;
          }
        },
        new ThreadPoolExecutor.CallerRunsPolicy() {
          @Override
          public void rejectedExecution(Runnable runnable,
                                        ThreadPoolExecutor e) {
            LOG.info("Execution for striped reading rejected, "
                + "Executing in current thread");
            // will run in the current thread
            super.rejectedExecution(runnable, e);
          }
        });

    stripedReadPool.allowCoreThreadTimeOut(true);
  }

  private void initializedTranscoderThreadPool(int numThreads) {
    stripedTranscoderPool = DFSUtilClient.getThreadPoolExecutor(numThreads,
        numThreads, 60, new LinkedBlockingQueue<>(),
        "StripedBlockTranscoder-", false);
    stripedTranscoderPool.allowCoreThreadTimeOut(true);
  }

  /**
   * Handles all currently pending transcoding tasks sent to this datanode.
   *
   * @param taskInfos BlockECReconstructionInfo
   *
   */
  public void processTranscoderTasks(
      Collection<BlockECTranscodeCommand.BlockECTranscodeInfo> taskInfos) {
    for (BlockECTranscodeInfo taskInfo : taskInfos) {
      try {
        final StripedBlockTranscoder task = new StripedBlockTranscoder(
            this, taskInfo.convert());
        stripedTranscoderPool.submit(task);
      } catch (Throwable e) {
        LOG.warn("Failed to transcode blocks", e);
      }
    }
  }

  DataNode getDatanode() {
    return datanode;
  }

  Configuration getConf() {
    return conf;
  }

  CompletionService<StripedBlockUtil.BlockReadStats> createReadService() {
    return new ExecutorCompletionService<>(stripedReadPool);
  }

  public void shutDown() {
    stripedTranscoderPool.shutdown();
    stripedReadPool.shutdown();
  }
}