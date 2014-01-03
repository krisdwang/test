// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import com.amazon.coral.service.*;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.model.NullModelIndex;
import java.util.List;
import java.util.LinkedList;

import org.junit.Test;
import static org.junit.Assert.*;
import org.mockito.Mockito;

public class CartesianProductKeyGeneratorTest {
  HandlerContext ctx = Mockito.mock(HandlerContext.class);

  @Test
  public void simple() {
    final List<CharSequence> firstPart = new LinkedList<CharSequence>() {{
      add("a");
      add("b");
    }};

    final List<CharSequence> secondPart = new LinkedList<CharSequence>() {{
      add("1");
      add("2");
      add("3");
    }};

    List<KeyGenerator> builders = new LinkedList<KeyGenerator>() {{
      add(new ListKeyGenerator(firstPart));
      add(new ListKeyGenerator(secondPart));
    }};

    KeyGenerator builder = new CartesianProductKeyGenerator(builders, "-");

    List<CharSequence> expected = new LinkedList<CharSequence>() {{
      add("a-1");
      add("a-2");
      add("a-3");
      add("b-1");
      add("b-2");
      add("b-3");
    }};

    List<CharSequence> actual = new LinkedList<CharSequence>();
    for (CharSequence key : builder.getKeys(ctx)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);

    builders = new LinkedList<KeyGenerator>() {{
      add(new ListKeyGenerator(secondPart));
      add(new ListKeyGenerator(firstPart));
    }};

    builder = new CartesianProductKeyGenerator(builders, "-");

    expected = new LinkedList<CharSequence>() {{
      add("1-a");
      add("1-b");
      add("2-a");
      add("2-b");
      add("3-a");
      add("3-b");
    }};

    actual = new LinkedList<CharSequence>();
    for (CharSequence key : builder.getKeys(ctx)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }

  @Test
  public void noGenerators() {
    List<KeyGenerator> builders = new LinkedList<KeyGenerator>();

    KeyGenerator builder = new CartesianProductKeyGenerator(builders, "-");

    List<CharSequence> actual = new LinkedList<CharSequence>();
    for (CharSequence key : builder.getKeys(ctx)) {
      actual.add(key);
    }
    assertTrue(actual.isEmpty());
  }

  @Test
  public void emptyKey() {
    final KeyGenerator empty = new ListKeyGenerator(new LinkedList<CharSequence>());
    final KeyGenerator letters = new ListKeyGenerator(new LinkedList<CharSequence>() {{
      add("foo");
      add("bar");
    }});

    List<KeyGenerator> builders = new LinkedList<KeyGenerator>() {{
      add(empty);
      add(letters);
    }};

    KeyGenerator builder = new CartesianProductKeyGenerator(builders, "-");

    List<CharSequence> expected = new LinkedList<CharSequence>();/* {{
      add("-foo");
      add("-bar");
    }};*/

    List<CharSequence> actual = new LinkedList<CharSequence>();
    for (CharSequence key : builder.getKeys(ctx)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);

    builders = new LinkedList<KeyGenerator>() {{
      add(letters);
      add(empty);
    }};

    builder = new CartesianProductKeyGenerator(builders, "-");

    expected = new LinkedList<CharSequence>();/* {{
      add("foo");
      add("bar");
    }};*/

    actual = new LinkedList<CharSequence>();
    for (CharSequence key : builder.getKeys(ctx)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }
}

class ListKeyGenerator extends KeyGenerator {
  private final List<CharSequence> keys;

  public ListKeyGenerator(List<CharSequence> keys) {
    if (keys == null)
      throw new IllegalArgumentException();
    this.keys = keys;
  }

  public ListKeyGenerator(CharSequence... keys) {
    if (keys == null)
      throw new IllegalArgumentException();
    this.keys = new LinkedList<CharSequence>();
    for (int i = 0; i < keys.length; i++) {
      this.keys.add(keys[i]);
    }
  }

  @Override
  public Iterable<CharSequence> getKeys(HandlerContext ctx) {
    return keys;
  }
}
