// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import com.amazon.coral.service.Job;
import com.amazon.coral.util.TextSequenceBuilder;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Collections;

/** @Deprecated Prefer com.amazon.coral.avail.key.CartesianProductKeyGenerator */
@Deprecated
public class CrossProductKeyBuilder implements KeyBuilder {
  private final List<KeyBuilder> builders;
  private final CharSequence delimiter;

  public CrossProductKeyBuilder(List<KeyBuilder> builders, CharSequence delimiter) {
    if (builders == null)
      throw new IllegalArgumentException();
    if (delimiter == null)
      throw new IllegalArgumentException();
    this.builders = Collections.unmodifiableList(new ArrayList<KeyBuilder>(builders));
    this.delimiter = delimiter;
  }

  @Override
  public Iterable<CharSequence> getKeys(Job job) {
    List<CharSequence> keys = new LinkedList<CharSequence>();
    if (builders.isEmpty())
      return keys;

    KeyBuilder seed = builders.get(0);
    for (CharSequence key : seed.getKeys(job)) {
      keys.add(key);
    }

    for (int i = 1; i < builders.size(); i++) {
      List<CharSequence> newKeys = new LinkedList<CharSequence>();
      Iterable<CharSequence> nextSegments = builders.get(i).getKeys(job);
      for (CharSequence oldKey : keys) {
        for (CharSequence nextSegment : nextSegments) {
          TextSequenceBuilder newKey = new TextSequenceBuilder();
          newKey
            .append(oldKey)
            .append(delimiter)
            .append(nextSegment);
          newKeys.add(newKey.toTextSequence());
        }
      }
      keys = newKeys;
    }

    return keys;
  }
}
