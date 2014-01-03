// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;

import com.amazon.coral.service.Job;
import com.amazon.coral.service.HandlerContext;

public class KeyGeneratorAppender extends KeyGenerator {
  private final KeyGenerator kg;
  private final String extraKey;

  public KeyGeneratorAppender(KeyGenerator kg, String extraKey) {
    this.kg = kg;
    this.extraKey = extraKey;
  }
  public Iterable<CharSequence> getKeys(HandlerContext ctx) {
    List<CharSequence> keys = new ArrayList<CharSequence>((Collection<CharSequence>)kg.getKeys(ctx));
    keys.add(extraKey);
    return keys;
  }
}
