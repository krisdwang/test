// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import com.amazon.coral.service.Job;

/** @Deprecated prefer com.amazon.coral.avail.key.KeyGenerator */
public interface KeyBuilder {
  public Iterable<CharSequence> getKeys(Job job);
}
