// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import java.util.ArrayList;
import java.util.List;

/**
 * A prefix key builder is a decorating {@link KeyBuilder} that applies a static
 * character sequence prefix to each of the keys returned by the type it
 * decorates.
 *
 * @author markbidd
 * @Deprecated prefer com.amazon.coral.avail.key.PrefixKeyGenerator
 */
@Deprecated
public final class PrefixKeyBuilder extends CrossProductKeyBuilder {

  /**
   * Initializes a new key builder that prepends the provided prefix and
   * delimiter to each of the keys from the provided builder. None of the
   * provided arguments can be <code>null</code>.
   */
  public PrefixKeyBuilder(CharSequence prefix, CharSequence delimeter, KeyBuilder builder) {
    super(buildList(prefix, builder), delimeter);
  }

  private static List<KeyBuilder> buildList(CharSequence prefix, KeyBuilder builder) {
    ArrayList<KeyBuilder> list = new ArrayList<KeyBuilder>(2);
    list.add(new StaticKeyBuilder(prefix));
    list.add(builder);

    return list;
  }

  /**
   * Creates a prefix string with the specified prefix, or returns the same KeyBuilder if the prefix was empty
   * @param builder
   * @param prefix
   * @param delimiter
   * @return
   */
  public static KeyBuilder maybePrefix(KeyBuilder builder, String prefix, String delimiter) {
    if(prefix == null)
      throw new IllegalArgumentException();
    if(prefix.isEmpty())
      return builder;
    return new PrefixKeyBuilder(prefix, delimiter, builder);
  }
}
