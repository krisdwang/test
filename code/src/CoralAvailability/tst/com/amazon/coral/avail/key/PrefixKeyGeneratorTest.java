// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.mockito.Mockito;

import java.util.Iterator;

import org.junit.Test;

public class PrefixKeyGeneratorTest {

  @Test
  public void prefixHappens() {
    PrefixKeyGenerator builder = new PrefixKeyGenerator("asdf", ";", new StaticKeyGenerator("ORIG"));
    Iterable<CharSequence> keys = builder.getKeys(null);
    Iterator<CharSequence> it = keys.iterator();

    assertTrue("asdf;ORIG".contentEquals(it.next()));
    assertFalse(it.hasNext());
  }

  @Test
  public void maybePrefixHappens() {
    KeyGenerator builder = PrefixKeyGenerator.maybePrefix(new StaticKeyGenerator("ORIG"), "asdf", ";");
    Iterable<CharSequence> keys = builder.getKeys(null);
    Iterator<CharSequence> it = keys.iterator();

    assertTrue("asdf;ORIG".contentEquals(it.next()));
    assertFalse(it.hasNext());
  }

  @Test
  public void emptyDelimiterHappens() {
    KeyGenerator builder = PrefixKeyGenerator.maybePrefix(new StaticKeyGenerator("ORIG"), "asdf", "");
    Iterable<CharSequence> keys = builder.getKeys(null);
    Iterator<CharSequence> it = keys.iterator();

    assertTrue("asdfORIG".contentEquals(it.next()));
    assertFalse(it.hasNext());
  }

  @Test
  public void prefixDoesntHappen() {
    KeyGenerator origBuilder = new StaticKeyGenerator("ORIG");
    KeyGenerator builder = PrefixKeyGenerator.maybePrefix(origBuilder, "", "IGNORED");
    Iterable<CharSequence> keys = builder.getKeys(null);
    Iterator<CharSequence> it = keys.iterator();

    assertTrue("ORIG".contentEquals(it.next()));
    assertTrue(builder == origBuilder);
    assertFalse(it.hasNext());
  }

  @Test(expected=RuntimeException.class)
  public void throwsIfPrefixNull() {
    PrefixKeyGenerator.maybePrefix(new StaticKeyGenerator("ORIG"), null, ";");
  }

  @Test(expected=RuntimeException.class)
  public void throwsIfDelimiterNull() {
    PrefixKeyGenerator.maybePrefix(new StaticKeyGenerator("ORIG"), "sigh", null);
  }
}
