// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import com.amazon.coral.service.Job;
import com.amazon.coral.service.HandlerContext;

public abstract class KeyGenerator {
  /** Return emptyList by default. */
  public abstract Iterable<CharSequence> getKeys(HandlerContext ctx);
}
