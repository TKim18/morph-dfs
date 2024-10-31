package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map;


public class RedundancyGroupMap {

  // TODO: to conserve memory, we can have <Long, Short> to maintain difference
  /** using a List here assuming that redundancy groups are sorted ascending */
  private final ListMultimap<Long, Long> map;

  public RedundancyGroupMap() {
    this.map = ArrayListMultimap.create();
  }

  public void add(long stripedId, long groupedId) {
    map.put(stripedId, groupedId);
  }

  public void remove(long stripedId) {
    map.removeAll(stripedId);
  }

  public void merge(RedundancyGroupMap other) {
    // we assume that there is never overlap between keys
    assert(Sets.intersection(other.getMap().keySet(), map.keySet()).isEmpty());
    map.putAll(other.getMap());
  }

  public List<Long> get(long stripedId) {
    return map.get(stripedId);
  }

  public int size() {
    return map.size();
  }

  private ListMultimap<Long, Long> getMap() {
    return map;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Map<Long, Collection<Long>> m = map.asMap();
    for (Map.Entry<Long, Collection<Long>> entry : m.entrySet()) {
      sb.append("Striped Block Id = ")
        .append(entry.getKey())
        .append(" | Mapped Group Ids = ");
      for (Long group : entry.getValue()) {
        sb.append(group).append(" : ");
      }
      sb.append("\n");
    }
    return sb.toString();
  }
}
