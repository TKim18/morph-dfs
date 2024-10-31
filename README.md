# Morph

Morph is a distributed filesystem design that achieves IO-efficient lifetime redundancy transitions.
This repo is an implementation of HDFS that is augmented with the key ideas of Morph.

## Hybrid Redundancy
Morph implements a form of hybrid redundancy which uses replication and erasure-coding to protect the data. 
Morph allows writing directly into hybrid form, either with the data striped (1 MB cells) or with contiguous chunks (8 MB cells for 8 MB chunks).
The write path is mostly implemented in:
* Client write: `hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSHybridOutputStream.java`
* Client write streamer: `hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/HybridDataStreamer.java`
* Datanode write handling: `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataXceiver.java`
* Namenode write handling: `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java`
* Namenode hybrid block metadata: `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoHybrid.java`

The read path is mostly implemented in:
* Client read: `hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSHybridInputStream.java`.

## Convertible Codes
Morph implements convertible codes in:
* Encoding: `hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/erasurecode/rawcoder/CCRawEncoder.java`
* Decoding: `hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/erasurecode/rawcoder/CCRawDecoder.java`

## Redundancy Transitions (Transcode)
Morph implements redundancy transitions in:
* Client exposed interface: `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/ectransitioner/ECTransitioner.java`
* Namenode implementation: `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirErasureCodingOp.java`

# More details
Please reach out to me at ttk2@cs.cmu.edu for questions or concerns.