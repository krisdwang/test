// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;

import com.amazon.coral.google.common.base.Preconditions;

import com.amazon.coral.service.HandlerContext;
import com.amazon.coral.service.Identity;
import com.amazon.coral.util.TextSequence;

/** This can extract 'identity attributes' from the Identity object
 * on a HandlerContext, via getIdentity(). */
public class IdentityKeyGenerator extends KeyGenerator {
  /**
   * A list of the identity attributes that are available from an
   * {@link IdentityKeyGenerator} by default (i.e., when not using the
   * {@link #IdentityKeyGenerator(List)} constructor). Currently, this list
   * includes the following keys:
   * <ul>
   *  <li>{@link Identity#AWS_ACCOUNT AWS_ACCOUNT}</li>
   *  <li>{@link Identity#AWS_ACCESS_KEY AWS_ACCESS_KEY}</li>
   *  <li>{@link Identity#HTTP_REMOTE_ADDRESS HTTP_REMOTE_ADDRESS}</li>
   * </ul>
   */
  public static final List<String> DEFAULT_ATTRIBUTES = Arrays.asList(new String[]{Identity.AWS_ACCOUNT, Identity.AWS_ACCESS_KEY, Identity.HTTP_REMOTE_ADDRESS});

  private final List<String> attributes;

  public IdentityKeyGenerator() {
    this(DEFAULT_ATTRIBUTES);
  }

  public IdentityKeyGenerator(List<String> attributes) {
    Preconditions.checkNotNull(attributes);
    Preconditions.checkArgument(0 != attributes.size(),
        "You must specify at least one identity attribute to throttle on.");
    for (String attribute : attributes) {
      Preconditions.checkNotNull(attribute);
    }
    this.attributes = new ArrayList<String>(attributes);
  }

  @Override
  public Iterable<CharSequence> getKeys(HandlerContext ctx) {
    Identity identity = ctx.getIdentity();
    HashSet<CharSequence> keys = new HashSet<CharSequence>(); // uniquify

    for (String attribute : attributes) {
      CharSequence value = identity.getAttribute(attribute);
      if (value != null) {
        CharSequence key = TextSequence.newInstance(attribute, ":", value);
        keys.add(key);
      }
    }

    return keys;
  }
}
