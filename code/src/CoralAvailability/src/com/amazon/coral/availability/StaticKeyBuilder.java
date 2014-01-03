// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazon.coral.service.Job;

/**
 * A static key builder is a {@link KeyBuilder} that simply returns all of the
 * character sequences supplied to it at runtime.
 *
 * @author markbidd
 * @Deprecated prefer com.amazon.coral.avail.key.StaticKeyGenerator
 */
public final class StaticKeyBuilder implements KeyBuilder {

  private final List<CharSequence> keys;

  /**
   * Initializes a new static key builder that will return each of the supplied
   * keys from its {@link #getKeys(Job) getKeys} method.
   * @param keys Cannot be null, or contain any null entries.
   */
  public StaticKeyBuilder(CharSequence... keys) {
    if(keys == null)
      throw new IllegalArgumentException("keys cannot be null.");

    ArrayList<CharSequence> list = new ArrayList<CharSequence>(keys.length);
    for(CharSequence cs : keys)
      if(cs == null)
        throw new IllegalArgumentException("keys cannot contain a null element.");
      else
        list.add(cs);

    this.keys = Collections.unmodifiableList(list);
  }

  /**
   * Initializes a new static key builder that will return each of the supplied
   * keys from its {@link #getKeys(Job) getKeys} method.
   * @param keys Cannot be null, or contain any null entries.
   */
  public StaticKeyBuilder(Iterable<CharSequence> keys) {
    if(keys == null)
      throw new IllegalArgumentException("keys cannot be null.");

    ArrayList<CharSequence> list = new ArrayList<CharSequence>();
    for(CharSequence cs : keys)
      if(cs == null)
        throw new IllegalArgumentException("keys cannot contain a null element.");
      else
        list.add(cs);

    this.keys = Collections.unmodifiableList(list);
  }

  /**
   * Returns an iterable sequence of each of the keys provided to the
   * {@link StaticKeyBuilder}'s constructor.
   */
  @Override
  public Iterable<CharSequence> getKeys(Job job) { return keys; }
}
