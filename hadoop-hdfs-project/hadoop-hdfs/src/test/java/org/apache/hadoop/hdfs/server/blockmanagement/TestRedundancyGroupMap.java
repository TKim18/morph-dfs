package org.apache.hadoop.hdfs.server.blockmanagement;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestRedundancyGroupMap {

  @Test
  public void testMerge() {
    RedundancyGroupMap m1 = new RedundancyGroupMap();
    m1.add(321L, 321L);
    RedundancyGroupMap m2 = new RedundancyGroupMap();
    m2.add(123L, 123L);
    m1.merge(m2);

    assertEquals(1, m1.get(123L).size());
    assertEquals(2, m1.size());
  }

  @Test(expected = AssertionError.class)
  public void testMergeWithIntersect() {
    RedundancyGroupMap m1 = new RedundancyGroupMap();
    m1.add(123L, 123L);
    RedundancyGroupMap m2 = new RedundancyGroupMap();
    m2.add(123L, 321L);
    m1.merge(m2);
  }

  @Test
  public void testToString() {
    RedundancyGroupMap m1 = new RedundancyGroupMap();
    m1.add(123L, 123L);
    m1.add(123L, 345L);
    m1.add(123L, 789L);
    m1.add(3333L, 1111L);
    m1.add(3332L, 1111L);
    System.out.println(m1);
  }

}
