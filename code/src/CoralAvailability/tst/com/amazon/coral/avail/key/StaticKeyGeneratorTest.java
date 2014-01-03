// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Arrays;


public class StaticKeyGeneratorTest {
  // Test some stuff not tested by other tests.

  static void assertContentEquals(List<String> strings, Iterable<CharSequence> chars) {
    int i = 0;
    for (CharSequence cs : chars) {
      Assert.assertEquals(strings.get(i), cs.toString());
      ++i;
    }
    Assert.assertEquals(strings.size(), i);
  }

  @Test(expected=RuntimeException.class)
  public void listConstructorNull() {
    new StaticKeyGenerator((java.lang.Iterable<java.lang.CharSequence>)null);
  }
  @Test//(expected=RuntimeException.class)
  public void listConstructorNullElement() {
    List<CharSequence> keys = Arrays.<CharSequence>asList("foo", null, "bar");
    new StaticKeyGenerator(keys);
  }
  @Test
  public void listConstructor() {
    List<CharSequence> keys = Arrays.<CharSequence>asList("foo", "bar");
    KeyGenerator kg = new StaticKeyGenerator(keys);
    Assert.assertEquals(keys, kg.getKeys(null));
  }
}
