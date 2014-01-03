package com.amazon.coral.availability;

import com.amazon.coral.service.*;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.model.NullModelIndex;
import java.util.List;
import java.util.LinkedList;

import org.junit.Test;
import static org.junit.Assert.*;

public class CrossProductKeyBuilderTest {
  private final static Job job = new JobImpl(new NullModelIndex(), new RequestImpl(new NullMetricsFactory().newMetrics()));

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

    List<KeyBuilder> builders = new LinkedList<KeyBuilder>() {{
      add(new ListKeyBuilder(firstPart));
      add(new ListKeyBuilder(secondPart));
    }};

    KeyBuilder builder = new CrossProductKeyBuilder(builders, "-");

    List<CharSequence> expected = new LinkedList<CharSequence>() {{
      add("a-1");
      add("a-2");
      add("a-3");
      add("b-1");
      add("b-2");
      add("b-3");
    }};

    List<CharSequence> actual = new LinkedList<CharSequence>();
    for (CharSequence key : builder.getKeys(job)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);

    builders = new LinkedList<KeyBuilder>() {{
      add(new ListKeyBuilder(secondPart));
      add(new ListKeyBuilder(firstPart));
    }};

    builder = new CrossProductKeyBuilder(builders, "-");

    expected = new LinkedList<CharSequence>() {{
      add("1-a");
      add("1-b");
      add("2-a");
      add("2-b");
      add("3-a");
      add("3-b");
    }};

    actual = new LinkedList<CharSequence>();
    for (CharSequence key : builder.getKeys(job)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }

  @Test
  public void noBuilders() {
    List<KeyBuilder> builders = new LinkedList<KeyBuilder>();

    KeyBuilder builder = new CrossProductKeyBuilder(builders, "-");

    List<CharSequence> actual = new LinkedList<CharSequence>();
    for (CharSequence key : builder.getKeys(job)) {
      actual.add(key);
    }
    assertTrue(actual.isEmpty());
  }

  @Test
  public void emptyKey() {
    final KeyBuilder empty = new ListKeyBuilder(new LinkedList<CharSequence>());
    final KeyBuilder letters = new ListKeyBuilder(new LinkedList<CharSequence>() {{
      add("foo");
      add("bar");
    }});

    List<KeyBuilder> builders = new LinkedList<KeyBuilder>() {{
      add(empty);
      add(letters);
    }};

    KeyBuilder builder = new CrossProductKeyBuilder(builders, "-");

    List<CharSequence> expected = new LinkedList<CharSequence>();/* {{
      add("-foo");
      add("-bar");
    }};*/

    List<CharSequence> actual = new LinkedList<CharSequence>();
    for (CharSequence key : builder.getKeys(job)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);

    builders = new LinkedList<KeyBuilder>() {{
      add(letters);
      add(empty);
    }};

    builder = new CrossProductKeyBuilder(builders, "-");

    expected = new LinkedList<CharSequence>();/* {{
      add("foo");
      add("bar");
    }};*/

    actual = new LinkedList<CharSequence>();
    for (CharSequence key : builder.getKeys(job)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }
}

class ListKeyBuilder implements KeyBuilder {
  private final List<CharSequence> keys;

  public ListKeyBuilder(List<CharSequence> keys) {
    if (keys == null)
      throw new IllegalArgumentException();
    this.keys = keys;
  }

  public ListKeyBuilder(CharSequence... keys) {
    if (keys == null)
      throw new IllegalArgumentException();
    this.keys = new LinkedList<CharSequence>();
    for (int i = 0; i < keys.length; i++) {
      this.keys.add(keys[i]);
    }
  }

  @Override
  public Iterable<CharSequence> getKeys(Job job) {
    return keys;
  }
}
