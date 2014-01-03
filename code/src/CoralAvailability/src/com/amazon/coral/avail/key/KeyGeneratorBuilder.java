// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import java.util.ArrayList;
import com.amazon.coral.google.common.base.Preconditions;

/** Builder for KeyGenerators, typically used in ThrottlingHandlers.
 * <p>
 * <a href="https://w.amazon.com/?Coral/Manual/Availability/Throttling">https://w.amazon.com/?Coral/Manual/Availability/Throttling</a>
 * <p>
 */
public class KeyGeneratorBuilder {
  // by default, no global-key, prefix, delimiter, or identities
  String globalKey = null;
  String prefix = "";
  String delimiter = "";
  java.util.List<String> identities = new ArrayList<String>();

  /** Set up the KGB to produce keys by default in the standard way for
   * ThrottlingHandler. */
  public static KeyGeneratorBuilder newBuilderForThrottlingHandler() {
    return new KeyGeneratorBuilder()
      .withGlobalKey("")
      .withPrefix("")
      .withDelimiter("")
      .withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
  }
  /** Set up the KGB to produce keys by default in the standard way for
   * LoadShedHandler. */
  public static KeyGeneratorBuilder newBuilderForLoadShedHandler() {
    return new KeyGeneratorBuilder()
      .withGlobalKey("droppable")
      .withPrefix("droppable")
      .withDelimiter("-")
      .withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
  }

  /** Default is "".
   * Cannot be null.
   * Note that this will *not* apply to any GlobalKey (for backwards compatibility)*/
  public void setPrefix(String prefix) {
    Preconditions.checkNotNull(prefix, "prefix cannot be null");
    this.prefix = prefix;
  }
  /** Default delimiter is "" (for backwards compatibility).
   * Cannot be null.*/
  public void setDelimiter(String d) {
    Preconditions.checkNotNull(prefix, "delimiter cannot be null");
    this.delimiter = d;
  }
  /** Cannot be null, but empty is fine. */
  public void setIdentities(java.util.List<String> identityKeys) {
    Preconditions.checkNotNull(identityKeys, "identities cannot be null");
    this.identities = identityKeys;
  }
  /** Default is \"\".
   * null means "do not use a global key".
   * Note that the prefix 'droppable-" will *not* be applied to any GlobalKey.*/
  public void setGlobalKey(String gk) {
    this.globalKey = gk;
  }

  public KeyGeneratorBuilder withPrefix(String prefix) {
    setPrefix(prefix);
    return this;
  }
  public KeyGeneratorBuilder withDelimiter(String d) {
    setDelimiter(d);
    return this;
  }
  public KeyGeneratorBuilder withIdentities(java.util.List<String> identityKeys) {
    setIdentities(identityKeys);
    return this;
  }
  /** TODO: Verify that all identities are valid, here and in setters. */
  public KeyGeneratorBuilder withIdentity(String identityKey) {
    if (IdentityKeyGenerator.DEFAULT_ATTRIBUTES == identities) {
      identities = new ArrayList<String>();
    }
    identities.add(identityKey);
    return this;
  }
  public KeyGeneratorBuilder withGlobalKey(String gk) {
    setGlobalKey(gk);
    return this;
  }

  /**
   * Keys are generated of the form
   *   IdentityKey:IdentityValue
   * e.g.
   *   aws-account:1234234
   * or withPrefix("ID:")
   *   ID:aws-account:1234234
   */
  public KeyGenerator buildForIdentity() {
    KeyGenerator kb = new IdentityKeyGenerator(identities);
    if (null != globalKey) {
      return new KeyGeneratorAppender(
          PrefixKeyGenerator.maybePrefix(kb, prefix, delimiter), globalKey);
    } else {
      return
          PrefixKeyGenerator.maybePrefix(kb, prefix, delimiter);
    }
  }
  /**
   * Keys are generated of the form
   *   IdentityKey:IdentityValue,Operation:ServiceName/OperationName
   * e.g.
   *   aws-account:1234,Operation:WeatherService/GetWeather
   * or withPrefix("ID_OP:")
   *   ID_OP:aws-account:1234234,Operation:BigBirdService/GetItems
   */
  public KeyGenerator buildForIdentityOperation() {
    ArrayList<KeyGenerator> builders = new ArrayList<KeyGenerator>(2);
    builders.add(new IdentityKeyGenerator(identities));
    builders.add(new OperationKeyGenerator());
    KeyGenerator kb = new CartesianProductKeyGenerator(builders, ",");
    if (null != globalKey) {
      return new KeyGeneratorAppender(
          PrefixKeyGenerator.maybePrefix(kb, prefix, delimiter), globalKey);
    } else {
      return
          PrefixKeyGenerator.maybePrefix(kb, prefix, delimiter);
    }
  }
  /**
   * Keys are generated of the form
   *   Operation:ServiceName/OperationName,IdentityKey:IdentityValue
   * e.g.
   *   Operation:WeatherService/GetWeather,aws-account:1234
   * or withPrefix("OP_ID:")
   *   OP_ID:Operation:WeatherService/GetWeather,aws-account:1234
   */
  public KeyGenerator buildForOperationIdentity() {
    ArrayList<KeyGenerator> builders = new ArrayList<KeyGenerator>(2);
    builders.add(new OperationKeyGenerator());
    builders.add(new IdentityKeyGenerator(identities));
    KeyGenerator kb = new CartesianProductKeyGenerator(builders, ",");
    if (null != globalKey) {
      return new KeyGeneratorAppender(
          PrefixKeyGenerator.maybePrefix(kb, prefix, delimiter), globalKey);
    } else {
      return
          PrefixKeyGenerator.maybePrefix(kb, prefix, delimiter);
    }
  }
  /**
   * Keys are generated of the form
   *   Operation:ServiceName/OperationName
   * e.g.
   *   Operation:WeatherService/GetWeather
   * or withPrefix("OP:")
   *   OP:Operation:BigBirdService/GetItems
   */
  public KeyGenerator buildForOperation() {
    Preconditions.checkState(identities.size()==0 || identities==IdentityKeyGenerator.DEFAULT_ATTRIBUTES, "Identities cannot be specified for Operation.");
    KeyGenerator kb = new OperationKeyGenerator();
    if (null != globalKey) {
      return new KeyGeneratorAppender(
          PrefixKeyGenerator.maybePrefix(kb, prefix, delimiter), globalKey);
    } else {
      return
          PrefixKeyGenerator.maybePrefix(kb, prefix, delimiter);
    }
  }
}
