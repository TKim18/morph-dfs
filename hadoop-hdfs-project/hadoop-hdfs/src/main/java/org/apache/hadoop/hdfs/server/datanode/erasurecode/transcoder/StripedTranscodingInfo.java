package org.apache.hadoop.hdfs.server.datanode.erasurecode.transcoder;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class StripedTranscodingInfo {
  static final Logger LOG = LoggerFactory.getLogger(StripedTranscodingInfo.class);

  // data blocks and locs that will be protected by this new group
  private final ExtendedBlock dataBlocks;
  private final DatanodeInfo[] dataSources;
  private final int[] dataIdOffsets;
  private final long[] dataLengths;

  // target erasure coding policy
  private final ErasureCodingPolicy ecPolicy;

  // target blocks info
  private final ExtendedBlock targetBlock;
  private final DatanodeInfo[] targets;
  private final StorageType[] targetStorageTypes;
  private final String[] targetStorageIds;

  // inputMode field specifies if parities can be read instead of data
  // the below fields are only set if shortcut is true
  // this is only useful in the case of Convertible Codes + Merge Regime
  // for now assume parity blocks perfectly replace data blocks
  // so the division of data blocks to parity blocks = ngroups
  private final byte inputMode;
  private final short nGroups;  // num groups combined
  private final ExtendedBlock[] parityBlocks; // array of parity block ID's
  private final DatanodeInfo[] paritySources;

  private final ECSchema oldSchema;

  public StripedTranscodingInfo(ExtendedBlock dataBlocks, DatanodeInfo[] dataSources,
                                int[] dataIdOffsets, long[] dataLengths,
                                ErasureCodingPolicy ecPolicy, DatanodeInfo[] targets,
                                StorageType[] targetStorageTypes, String[] targetStorageIds,
                                ExtendedBlock[] parityBlocks, DatanodeInfo[] paritySources, ECSchema oldSchema) {
    // data inputs (protected data group)
    this.dataBlocks = dataBlocks;
    this.dataSources = dataSources;
    this.dataIdOffsets = dataIdOffsets;
    this.dataLengths = dataLengths;

    // target EC scheme
    this.ecPolicy = ecPolicy;

    // target output (new parity set)
    this.targetBlock = dataBlocks;
    this.targets = targets;
    this.targetStorageTypes = targetStorageTypes;
    this.targetStorageIds = targetStorageIds;

    //Old number of data blocks
    this.oldSchema = oldSchema;

    // parity inputs (useful for CC transcodings)
    if (parityBlocks != null && parityBlocks.length > 0) {
      if (Objects.equals(oldSchema.getCodecName(), "bcc")) {
        this.inputMode = 2;
      }
      else {
        this.inputMode = 1;
      }
      this.nGroups = (short) parityBlocks.length;
      this.parityBlocks = parityBlocks;
      this.paritySources = paritySources;
    }
    else {
      this.inputMode = 0;
      this.nGroups = 0;
      this.parityBlocks = null;
      this.paritySources = null;
    }
  }

  public ExtendedBlock getDataBlocks() {
    return dataBlocks;
  }

  public DatanodeInfo[] getDataSources() {
    return dataSources;
  }

  public ErasureCodingPolicy getEcPolicy() {
    return ecPolicy;
  }


  public byte getInputMode() {
    return inputMode;
  }

  public short getnGroups() {
    return nGroups;
  }

  public ExtendedBlock[] getParityBlocks() {
    return parityBlocks;
  }

  public DatanodeInfo[] getParitySources() {
    return paritySources;
  }

  public int[] getDataIdOffsets() {
    return dataIdOffsets;
  }

  public long[] getDataLengths() {
    return dataLengths;
  }

  public ExtendedBlock getTargetBlock() {
    return targetBlock;
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

  public ECSchema getOldSchema() {
    return oldSchema;
  }
}
