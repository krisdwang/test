package com.amazon.coral.availability;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;

import com.amazon.coral.service.Job;
import com.amazon.coral.service.Identity;
import com.amazon.coral.service.IdentityHelper;
import com.amazon.coral.util.TextSequence;

/** @Deprecated prefer com.amazon.coral.avail.key.IdentityKeyGenerator */
@Deprecated
public class IdentityKeyBuilder implements KeyBuilder {
  /**
   * A list of the identity attributes that are available from an
   * {@link IdentityKeyBuilder} by default (i.e., when not using the
   * {@link #IdentityKeyBuilder(List)} constructor). Currently, this list
   * includes the following keys:
   * <ul>
   *  <li>{@link Identity#AWS_ACCOUNT AWS_ACCOUNT}</li>
   *  <li>{@link Identity#AWS_ACCESS_KEY AWS_ACCESS_KEY}</li>
   *  <li>{@link Identity#HTTP_REMOTE_ADDRESS HTTP_REMOTE_ADDRESS}</li>
   * </ul>
   */
  public static final List<String> DEFAULT_ATTRIBUTES = Arrays.asList(new String[]{Identity.AWS_ACCOUNT, Identity.AWS_ACCESS_KEY, Identity.HTTP_REMOTE_ADDRESS});

  private final List<String> attributes;

  public IdentityKeyBuilder() {
    this(DEFAULT_ATTRIBUTES);
  }

  public IdentityKeyBuilder(List<String> attributes) {
    if (attributes == null)
      throw new IllegalArgumentException();
    if (attributes.size() == 0)
      throw new IllegalArgumentException("You must specify at least one identity attribute to throttle on.");
    for (String attribute : attributes) {
      if (attribute == null)
        throw new IllegalArgumentException();
    }
    this.attributes = new ArrayList<String>(attributes);
  }

  @Override
  public Iterable<CharSequence> getKeys(Job job) {
    Identity identity = IdentityHelper.getIdentity(job);
    HashSet<CharSequence> keys = new HashSet<CharSequence>();

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
