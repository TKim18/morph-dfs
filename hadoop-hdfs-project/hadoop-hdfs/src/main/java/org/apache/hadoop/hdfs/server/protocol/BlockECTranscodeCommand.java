package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.ExtendedGroup;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.transcoder.StripedTranscodingInfo;
import org.apache.hadoop.io.erasurecode.ECSchema;

import java.util.Collection;

/**
 * A BlockECTranscodeCommand is an instruction to a DataNode to
 * transcode a striped block group from one EC scheme to another.
 *
 * Upon receiving this command, the DataNode reads data from the input
 * sources and reconstructs the output blocks through codec calculation.
 *
 * After the new parities are computed, the DataNode pushes the new parity blocks
 * to their final destinations if necessary.
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockECTranscodeCommand extends DatanodeCommand {

  private final Collection<BlockECTranscodeInfo> transcodeTasks;

  public BlockECTranscodeCommand(int action,
       Collection<BlockECTranscodeInfo> tasks) {
    super(action);
    this.transcodeTasks = tasks;
  }

  public Collection<BlockECTranscodeInfo> getTranscodeTasks() {
    return this.transcodeTasks;
  }

  public static class BlockECTranscodeInfo {

    // data blocks and locs that will be protected by this new group
    private final ExtendedGroup group;
    private final DatanodeInfo[] dataSources;

    // target erasure coding policy
    private final ErasureCodingPolicy ecPolicy;

    // target blocks info
    private final DatanodeInfo[] targets;
    private final StorageType[] targetStorageTypes;
    private final String[] targetStorageIds;

    // shortcut fields
    private final ExtendedBlock[] parityBlocks;
    private final DatanodeInfo[] paritySources;

    private final ECSchema oldSchema;

    public BlockECTranscodeInfo(ExtendedGroup group, DatanodeInfo[] dataSources,
                                ErasureCodingPolicy ecPolicy, DatanodeStorageInfo[] targets,
                                ExtendedBlock[] parityBlocks, DatanodeInfo[] paritySources, ECSchema oldSchema) {
      this(group, dataSources, ecPolicy,
          DatanodeStorageInfo.toDatanodeInfos(targets),
          DatanodeStorageInfo.toStorageTypes(targets),
          DatanodeStorageInfo.toStorageIDs(targets),
          parityBlocks, paritySources, oldSchema);
    }

    public BlockECTranscodeInfo(ExtendedGroup group, DatanodeInfo[] dataSources,
                                ErasureCodingPolicy ecPolicy, DatanodeInfo[] targets,
                                StorageType[] targetStorageTypes, String[] targetStorageIds,
                                ExtendedBlock[] parityBlocks, DatanodeInfo[] paritySources, ECSchema oldSchema) {
      this.group = group;
      this.dataSources = dataSources;
      this.ecPolicy = ecPolicy;
      this.targets = targets;
      this.targetStorageTypes = targetStorageTypes;
      this.targetStorageIds = targetStorageIds;
      this.parityBlocks = parityBlocks;
      this.paritySources = paritySources;
      this.oldSchema = oldSchema;
    }

    public StripedTranscodingInfo convert() {
      return new StripedTranscodingInfo(
          group.getBlock(), dataSources,
          group.getBlockIdOffsets(),
          group.getBlockLengths(),
          ecPolicy, targets,
          targetStorageTypes, targetStorageIds,
          parityBlocks, paritySources, oldSchema);
    }

    public ExtendedGroup getGroup() {
      return group;
    }

    public DatanodeInfo[] getDataSources() {
      return dataSources;
    }

    public ErasureCodingPolicy getEcPolicy() {
      return ecPolicy;
    }

    public DatanodeInfo[] getTargets() {
      return targets;
    }

    public StorageType[] getTargetStorageTypes() {
      return targetStorageTypes;
    }

    public String[] getTargetStorageIds() {
      return targetStorageIds;
    }

    public ExtendedBlock[] getParityBlocks() {
      return parityBlocks;
    }

    public DatanodeInfo[] getParitySources() {
      return paritySources;
    }

    public ECSchema getOldSchema() {
      return oldSchema;
    }
  }
}
