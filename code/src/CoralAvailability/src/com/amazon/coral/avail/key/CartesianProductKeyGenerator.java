// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import com.amazon.coral.util.TextSequenceBuilder;
import com.amazon.coral.service.HandlerContext;

import com.amazon.coral.google.common.base.Preconditions;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Collections;

public class CartesianProductKeyGenerator extends KeyGenerator {
  private final List<KeyGenerator> generators;
  private final CharSequence delimiter;

  public CartesianProductKeyGenerator(List<KeyGenerator> generators, CharSequence delimiter) {
    Preconditions.checkNotNull(generators);
    Preconditions.checkNotNull(delimiter);
    this.generators = Collections.unmodifiableList(new ArrayList<KeyGenerator>(generators));
    this.delimiter = delimiter;
  }

  @Override
  public Iterable<CharSequence> getKeys(HandlerContext ctx) {
    List<CharSequence> keys = new LinkedList<CharSequence>();
    if (generators.isEmpty())
      return keys;

    KeyGenerator seed = generators.get(0);
    for (CharSequence key : seed.getKeys(ctx)) {
      keys.add(key);
    }

    for (int i = 1; i < generators.size(); i++) {
      List<CharSequence> newKeys = new LinkedList<CharSequence>();
      Iterable<CharSequence> nextSegments = generators.get(i).getKeys(ctx);
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
