// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import com.amazon.coral.service.HandlerContext;

import com.amazon.coral.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * A prefix key generator is a decorating {@link KeyGenerator} that applies a static
 * character sequence prefix to each of the keys returned by the type it
 * decorates.
 *
 * @author markbidd
 */
public final class PrefixKeyGenerator extends CartesianProductKeyGenerator {

  /**
   * Initializes a new key generator that prepends the provided prefix and
   * delimiter to each of the keys from the provided generator. None of the
   * provided arguments can be <code>null</code>.
   */
  public PrefixKeyGenerator(CharSequence prefix, CharSequence delimiter, KeyGenerator generator) {
    super(generatorList(prefix, generator), delimiter);
  }

  private static List<KeyGenerator> generatorList(CharSequence prefix, KeyGenerator generator) {
    Preconditions.checkNotNull(generator);
    Preconditions.checkNotNull(prefix);
    ArrayList<KeyGenerator> list = new ArrayList<KeyGenerator>(2);
    list.add(new StaticKeyGenerator(prefix));
    list.add(generator);

    return list;
  }

  /**
   * Creates a prefix string with the specified prefix, or returns the same KeyGenerator if the prefix was empty
   * @param generator
   * @param prefix
   * @param delimiter
   * @return
   */
  static KeyGenerator maybePrefix(KeyGenerator generator, String prefix, String delimiter) {
    if(null!=prefix && prefix.isEmpty()) {
      return generator;
    }
    return new PrefixKeyGenerator(prefix, delimiter, generator);
  }
}
