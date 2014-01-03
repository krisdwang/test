// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazon.coral.google.common.base.Preconditions;

import com.amazon.coral.service.HandlerContext;

/**
 * A {@link KeyGenerator} that simply returns all of the
 * character sequences supplied to it at runtime.
 *
 * @author markbidd
 */
public final class StaticKeyGenerator extends KeyGenerator {

  private final List<CharSequence> keys;

  /**
   * Initializes a new static key generator that will return each of the supplied
   * keys from its {@link #getKeys(Job) getKeys} method.
   * @param keys Cannot contain any null entries.
   */
  public StaticKeyGenerator(CharSequence... keys) {
    ArrayList<CharSequence> list = new ArrayList<CharSequence>(keys.length);
    for(CharSequence cs : keys) {
      Preconditions.checkNotNull(cs);
      list.add(cs);
    }
    this.keys = Collections.unmodifiableList(list);
  }

  /**
   * Initializes a new static key generator that will return each of the supplied
   * keys from its {@link #getKeys(Job) getKeys} method.
   * @param keys Cannot be null, or contain any null entries.
   */
  public StaticKeyGenerator(Iterable<CharSequence> keys) {
    Preconditions.checkNotNull(keys);
    ArrayList<CharSequence> list = new ArrayList<CharSequence>();
    for(CharSequence cs : keys) {
      Preconditions.checkNotNull(keys);
      list.add(cs);
    }
    this.keys = Collections.unmodifiableList(list);
  }

  /**
   * Returns an iterable sequence of each of the keys provided to the
   * {@link StaticKeyBuilder}'s constructor.
   */
  @Override
  public Iterable<CharSequence> getKeys(HandlerContext ctx) { return keys; }
}
