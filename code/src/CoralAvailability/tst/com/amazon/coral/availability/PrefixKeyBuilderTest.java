package com.amazon.coral.availability;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

public class PrefixKeyBuilderTest {

  @Test
  public void prefixHappens() {
    PrefixKeyBuilder builder = new PrefixKeyBuilder("asdf", ";", new StaticKeyBuilder("ORIG"));
    Iterable<CharSequence> keys = builder.getKeys(null);
    Iterator<CharSequence> it = keys.iterator();

    assertTrue("asdf;ORIG".contentEquals(it.next()));
    assertFalse(it.hasNext());
  }

  @Test
  public void maybePrefixHappens() {
    KeyBuilder builder = PrefixKeyBuilder.maybePrefix(new StaticKeyBuilder("ORIG"), "asdf", ";");
    Iterable<CharSequence> keys = builder.getKeys(null);
    Iterator<CharSequence> it = keys.iterator();

    assertTrue("asdf;ORIG".contentEquals(it.next()));
    assertFalse(it.hasNext());
  }

  @Test
  public void emptyDelimiterHappens() {
    KeyBuilder builder = PrefixKeyBuilder.maybePrefix(new StaticKeyBuilder("ORIG"), "asdf", "");
    Iterable<CharSequence> keys = builder.getKeys(null);
    Iterator<CharSequence> it = keys.iterator();

    assertTrue("asdfORIG".contentEquals(it.next()));
    assertFalse(it.hasNext());
  }

  @Test
  public void prefixDoesntHappen() {
    KeyBuilder origBuilder = new StaticKeyBuilder("ORIG");
    KeyBuilder builder = PrefixKeyBuilder.maybePrefix(origBuilder, "", "IGNORED");
    Iterable<CharSequence> keys = builder.getKeys(null);
    Iterator<CharSequence> it = keys.iterator();

    assertTrue("ORIG".contentEquals(it.next()));
    assertTrue(builder == origBuilder);
    assertFalse(it.hasNext());
  }

  @Test(expected=IllegalArgumentException.class)
  public void throwsIfPrefixNull() {
    PrefixKeyBuilder.maybePrefix(new StaticKeyBuilder("ORIG"), null, ";");
  }

  @Test(expected=IllegalArgumentException.class)
  public void throwsIfDelimiterNull() {
    PrefixKeyBuilder.maybePrefix(new StaticKeyBuilder("ORIG"), "sigh", null);
  }
}
