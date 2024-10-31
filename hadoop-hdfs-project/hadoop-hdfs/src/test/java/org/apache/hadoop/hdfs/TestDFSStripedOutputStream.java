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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumSet;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.server.ectransitioner.ECTransitioner;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities.StreamCapability;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class TestDFSStripedOutputStream {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestDFSStripedOutputStream.class);

  static {
    GenericTestUtils.setLogLevel(DFSOutputStream.LOG, Level.INFO);
    GenericTestUtils.setLogLevel(DataStreamer.LOG, Level.INFO);
  }

  private ErasureCodingPolicy ecPolicy;
  private int dataBlocks;
  private int parityBlocks;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf;
  private int cellSize;
  private final int stripesPerBlock = 8;
  private int blockSize;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  public ErasureCodingPolicy getEcPolicy() {
    // return SystemErasureCodingPolicies.getPolicies().get(SystemErasureCodingPolicies.LRC_12_2_2_POLICY_ID - 1);
    // return SystemErasureCodingPolicies.getPolicies().get(SystemErasureCodingPolicies.BCC_6_1_2_POLICY_ID - 1);
    return SystemErasureCodingPolicies.getPolicies().get(SystemErasureCodingPolicies.RS_6_3_POLICY_ID - 1);
    // return SystemErasureCodingPolicies.getPolicies().get(SystemErasureCodingPolicies.CC_6_3_POLICY_ID - 1);
    // return StripedFileTestUtil.getDefaultECPolicy();
  }

  @Before
  public void setup() throws IOException {
    /*
     * Initialize erasure coding policy.
     */
    ecPolicy = getEcPolicy();
    dataBlocks = (short) ecPolicy.getNumDataUnits();
    parityBlocks = (short) ecPolicy.getNumParityUnits();
    cellSize = ecPolicy.getCellSize();
    blockSize = stripesPerBlock * cellSize;
    System.out.println("EC policy = " + ecPolicy);

    int replication = 3;
    int numDNs = 15;
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    // set default replication for testing
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, replication);
    // enable hedged reads
    conf.setInt(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY, replication);
    conf.setInt(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY, 400);
    // used to control ack queue size
    conf.setInt(HdfsClientConfigKeys.Write.MAX_PACKETS_IN_FLIGHT_KEY, 300);
    conf.setInt(HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY, 8);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
          CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
          NativeRSRawErasureCoderFactory.CODER_NAME);
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    fs = cluster.getFileSystem();
    DFSTestUtil.enableAllECPolicies(fs);
    fs.getClient().setErasureCodingPolicy("/", ecPolicy.getName());
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testFileEmpty() throws Exception {
    testOneFile("/EmptyFile", 0);
  }

  @Test
  public void testFileSmallerThanOneCell1() throws Exception {
    testOneFile("/SmallerThanOneCell", 1);
  }

  @Test
  public void testFileSmallerThanOneCell2() throws Exception {
    testOneFile("/SmallerThanOneCell", cellSize - 1);
  }

  @Test
  public void testFileEqualsWithOneCell() throws Exception {
    testOneFile("/EqualsWithOneCell", cellSize);
  }

  @Test
  public void testFileSmallerThanOneStripe1() throws Exception {
    testOneFile("/SmallerThanOneStripe", cellSize * dataBlocks - 1);
  }

  @Test
  public void testFileSmallerThanOneStripe2() throws Exception {
    testOneFile("/SmallerThanOneStripe", cellSize + 123);
  }

  @Test
  public void testFileEqualsWithOneStripe() throws Exception {
    testOneFile("/EqualsWithOneStripe", cellSize * dataBlocks);
  }

  @Test
  public void testFileMoreThanOneStripe1() throws Exception {
    testOneFile("/MoreThanOneStripe1", cellSize * dataBlocks + 123);
  }

  @Test
  public void testFileMoreThanOneStripe2() throws Exception {
    testOneFile("/MoreThanOneStripe2", cellSize * dataBlocks
        + cellSize * dataBlocks + 123);
  }

  @Test
  public void testFileLessThanFullBlockGroup() throws Exception {
    testOneFile("/LessThanFullBlockGroup",
        cellSize * dataBlocks * (stripesPerBlock - 1) + cellSize);
  }

  @Test
  public void testFileFullBlockGroup() throws Exception {
    int fileSize = 2 * blockSize * dataBlocks;
//    int fileSize = blockSize - 1;
//    multiTest("/hybrid_FullBlockGroup", fileSize);
    testOneFile("/hybrid_FullBlockGroup", fileSize);
//    fs.dfs.setErasureCodingPolicy("/FullBlockGroup_" + fileSize, "CC-12-3-1024k");
//    Thread.sleep(10000);
//    LocatedBlocks lbs = fs.getClient().getLocatedBlocks(
//            "/FullBlockGroup_" + fileSize, 0L, Long.MAX_VALUE);
  }

  @Test
  public void testFileMoreThanABlockGroup1() throws Exception {
    testOneFile("/MoreThanABlockGroup1", blockSize * dataBlocks + 123);
  }

  @Test
  public void testFileMoreThanABlockGroup2() throws Exception {
    testOneFile("/MoreThanABlockGroup2",
        blockSize * dataBlocks + cellSize+ 123);
  }

  @Test
  public void testFileMoreThanABlockGroup35() throws Exception {
    testOneFile("/MoreThanABlockGroup3",
        blockSize * dataBlocks * 4);
  }

  @Test
  public void testFileMoreThanABlockGroup4() throws Exception {
    testOneFile("/MoreThanABlockGroup3",
        blockSize * dataBlocks * 3 + cellSize * dataBlocks
            + cellSize + 123);
  }

  /**
   * {@link DFSStripedOutputStream} doesn't support hflush() or hsync() yet.
   * This test is to make sure that DFSStripedOutputStream doesn't throw any
   * {@link UnsupportedOperationException} on hflush() or hsync() so as to
   * comply with output stream spec.
   *
   * @throws Exception
   */
  @Test
  public void testStreamFlush() throws Exception {
    final byte[] bytes = StripedFileTestUtil.generateBytes(blockSize *
        dataBlocks * 3 + cellSize * dataBlocks + cellSize + 123);
    try (FSDataOutputStream os = fs.create(new Path("/ec-file-1"))) {
      assertFalse(
          "DFSStripedOutputStream should not have hflush() capability yet!",
          os.hasCapability(StreamCapability.HFLUSH.getValue()));
      assertFalse(
          "DFSStripedOutputStream should not have hsync() capability yet!",
          os.hasCapability(StreamCapability.HSYNC.getValue()));
      try (InputStream is = new ByteArrayInputStream(bytes)) {
        IOUtils.copyBytes(is, os, bytes.length);
        os.hflush();
        IOUtils.copyBytes(is, os, bytes.length);
        os.hsync();
        IOUtils.copyBytes(is, os, bytes.length);
      }
      assertTrue("stream is not a DFSStripedOutputStream",
          os.getWrappedStream() instanceof DFSStripedOutputStream);
      final DFSStripedOutputStream dfssos =
          (DFSStripedOutputStream) os.getWrappedStream();
      dfssos.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
    }
  }

  private void multiTest(String base, int writeBytes) throws Exception {
    final byte[] bytes = StripedFileTestUtil.generateBytes(writeBytes);
    for (int t = 0; t < 25; t++) {
      // t = number of threads
      final int index = t;
      Thread thread = new Thread(() -> {
        for (int i = 0; i < 30; i++) {
          final Path testPath = new Path(base + "_" + i + "_" + index);
          try {
            DFSTestUtil.writeFile(
                fs, testPath, new String(bytes));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      thread.start();
    }
  }

  private void testOneFile(String src, int writeBytes) throws Exception {
    src += "_" + writeBytes;

    byte[] bytes = StripedFileTestUtil.generateBytes(writeBytes);
    long runningsum = 0;
    for (int i = 0; i < 1; i++) {
      final long start = System.currentTimeMillis();
      LOG.error("Starting to write file");
      Path testPath = new Path(src + i);
      DFSTestUtil.writeFile(fs, testPath, new String(bytes));
      final long wait = System.currentTimeMillis();
      runningsum += (wait - start);
      LOG.error("TTT = " + (wait - start) + " ms. Time to wait = "
          + (System.currentTimeMillis() - wait) + " ms.");

      // test transitions
//      ErasureCodingPolicy hybridOnePolicy = new ErasureCodingPolicy(
//          new ECSchema("XOR", 2,1), cellSize);
//      ErasureCodingPolicy fullEcPolicy = new ErasureCodingPolicy(
//          new ECSchema("CC", 6,3), cellSize);
//      ErasureCodingPolicy targetEcPolicy = new ErasureCodingPolicy(
//          new ECSchema("CC", 12,3), cellSize);
//      fs.getClient().getNamenode().setErasureCodingPolicy(src + i, hybridOnePolicy.getName());
//      Thread.sleep(2000);
//      fs.getClient().getNamenode().setErasureCodingPolicy(src + i, fullEcPolicy.getName());
//      Thread.sleep(2000);
//      fs.getClient().getNamenode().setErasureCodingPolicy(src + i, targetEcPolicy.getName());
//      Thread.sleep(10000);
//      fs.getClient().getNamenode().setErasureCodingPolicy(src + i, farthestEcPolicy.getName());
//      Thread.sleep(2000);
//      LOG.info("SHUTTING OFF DATANODE {}", cluster.getDataNodes().get(0).getXferAddress());
//      cluster.stopDataNode(DFSHybridOutputStream.datanodeToKill.getXferAddr());
//      cluster.stopDataNode(DFSHybridOutputStream.datanodeToKill2.getXferAddr());
//      cluster.stopDataNode(DFSHybridOutputStream.datanodeToKill3.getXferAddr());
//      cluster.stopDataNode(0);
//      cluster.stopDataNode(1);
//      cluster.stopDataNode(2);
//      cluster.stopDataNode(3);
//      cluster.stopDataNode(4);
//      cluster.stopDataNode(5);
//
      try (DFSInputStream in = fs.getClient().open(testPath.toString())) {
        int len = 8*1024*1024;
        byte[] b = new byte[len];
        int offset = 0;
        offset += in.read(len*0, b, 0, len);
//        assert x == len;
      }
    }
    Thread.sleep(1000 * 1200);
//    LOG.info("TTT_10 = {} ms", runningsum/10);
//    StripedFileTestUtil.waitBlockGroupsReported(fs, src);

//    StripedFileTestUtil.checkData(fs, testPath, writeBytes,
//        new ArrayList<DatanodeInfo>(), null, blockSize * dataBlocks);

    // we can try to read the block here maybe?
    if (false) {
      // can be used to check if partial parity implementation correctly outputs the right parities
      StripedFileTestUtil.checkManualParity(fs, null, writeBytes,
          new ArrayList<DatanodeInfo>(), null, blockSize * dataBlocks);
    }
  }

  @Test
  public void testFileBlockSizeSmallerThanCellSize() throws Exception {
    final Path path = new Path("testFileBlockSizeSmallerThanCellSize");
    final byte[] bytes = StripedFileTestUtil.generateBytes(cellSize * 2);
    try {
      DFSTestUtil.writeFile(fs, path, bytes, cellSize / 2);
      fail("Creating a file with block size smaller than "
          + "ec policy's cell size should fail");
    } catch (IOException expected) {
      LOG.info("Caught expected exception", expected);
      GenericTestUtils
          .assertExceptionContains("less than the cell size", expected);
    }
  }
}
