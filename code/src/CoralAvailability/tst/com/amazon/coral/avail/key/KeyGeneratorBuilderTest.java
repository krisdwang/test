// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import org.junit.*;
import org.mockito.Mockito;
import java.util.*;
import com.amazon.coral.service.*;
import com.amazon.coral.google.common.collect.Lists;


public class KeyGeneratorBuilderTest {
    KeyGeneratorBuilder kgb;
    List<String> empty = new ArrayList<String>();
    KeyGenerator kg;
    List<String> expected;

    <T extends CharSequence> ArrayList<String> sorted(Iterable<T> i) {
      ArrayList<String> al = unsorted(i);
      Collections.sort(al);
      return al;
    }

    <T extends CharSequence> ArrayList<String> unsorted(Iterable<T> i) {
      ArrayList<String> al = new ArrayList<String>();
      for (T s : i) {
        al.add(s.toString());
      }
      return al;
    }

    @Test
    public void defaults() {
      kgb = new KeyGeneratorBuilder();
      Assert.assertEquals("", kgb.prefix);
      Assert.assertEquals(null, kgb.globalKey);
      Assert.assertEquals("", kgb.delimiter);
      Assert.assertEquals(empty, kgb.identities);
    }
    @Test
    public void defaultsForThrottlingHandler() {
      kgb = KeyGeneratorBuilder.newBuilderForThrottlingHandler();
      Assert.assertEquals("", kgb.prefix);
      Assert.assertEquals("", kgb.globalKey);
      Assert.assertEquals("", kgb.delimiter);
      Assert.assertEquals(IdentityKeyGenerator.DEFAULT_ATTRIBUTES, kgb.identities);
    }
    @Test
    public void defaultsForLoadShedHandler() {
      kgb = KeyGeneratorBuilder.newBuilderForLoadShedHandler();
      Assert.assertEquals("droppable", kgb.prefix);
      Assert.assertEquals("droppable", kgb.globalKey);
      Assert.assertEquals("-", kgb.delimiter);
      Assert.assertEquals(IdentityKeyGenerator.DEFAULT_ATTRIBUTES, kgb.identities);
    }
    @Test
    public void setters() {
      kgb = new KeyGeneratorBuilder();

      kgb.setPrefix("test1");
      Assert.assertEquals("test1", kgb.prefix);
      kgb.withPrefix("wtest1");
      Assert.assertEquals("wtest1", kgb.prefix);

      kgb.setGlobalKey("test2");
      Assert.assertEquals("test2", kgb.globalKey);
      kgb.withGlobalKey("wtest2");
      Assert.assertEquals("wtest2", kgb.globalKey);

      kgb.setDelimiter("test3");
      Assert.assertEquals("test3", kgb.delimiter);
      kgb.withDelimiter("wtest3");
      Assert.assertEquals("wtest3", kgb.delimiter);

      kgb.setIdentities(empty);
      Assert.assertEquals(empty, kgb.identities);
      kgb.withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      Assert.assertEquals(IdentityKeyGenerator.DEFAULT_ATTRIBUTES, kgb.identities);
    }

    @Test
    public void missingIdentities_buildForIdentity() {
      try {
        new KeyGeneratorBuilder().buildForIdentity();
        Assert.fail("should throw");
      } catch (RuntimeException e) {
        Assert.assertEquals("You must specify at least one identity attribute to throttle on.",
            e.getMessage());
      }
    }

    @Test
    public void missingIdentities_buildForIdentityOperation() {
      try {
        new KeyGeneratorBuilder().buildForIdentityOperation();
        Assert.fail("should throw");
      } catch (RuntimeException e) {
        Assert.assertEquals("You must specify at least one identity attribute to throttle on.",
            e.getMessage());
      }
    }

    @Test
    public void missingIdentities_buildForOperationIdentity() {
      try {
        new KeyGeneratorBuilder().buildForOperationIdentity();
        Assert.fail("should throw");
      } catch (RuntimeException e) {
        Assert.assertEquals("You must specify at least one identity attribute to throttle on.",
            e.getMessage());
      }
    }

    @Test
    public void tmi_buildForOperation() {
      try {
        new KeyGeneratorBuilder()
          .withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES)
          .withIdentity("foo")
          .withIdentity("bar")
          .buildForOperation();
        Assert.fail("should throw");
      } catch (RuntimeException e) {
        Assert.assertEquals("Identities cannot be specified for Operation.",
            e.getMessage());
      }
    }

    @Test
    public void default_buildForIdentity() {
      Identity id= new Identity();
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey("foo").buildForIdentity();
      expected = Arrays.asList("foo"); // global key only
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }
    @Test
    public void defaultSansGlobal_buildForIdentity() {
      Identity id= new Identity();
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey(null).buildForIdentity();
      expected = Arrays.<String>asList(); // not even the global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }
    @Test
    public void stuff_buildForIdentity() {
      Identity id= new Identity();
      id.setAttribute(Identity.AWS_ACCOUNT, "fake1");
      id.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "fake2");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey("foo").buildForIdentity();
      expected = Arrays.asList("foo", "aws-account:fake1", "http-remote-address:fake2"); // with global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }
    @Test
    public void stuffSansGlobal_buildForIdentity() {
      Identity id= new Identity();
      id.setAttribute(Identity.AWS_ACCOUNT, "fake1");
      id.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "fake2");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey(null).buildForIdentity();
      expected = Arrays.asList("aws-account:fake1", "http-remote-address:fake2"); // exclude global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }

    @Test
    public void default_buildForIdentityOperation() {
      Identity id= new Identity();
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey("foo").buildForIdentityOperation();
      expected = Arrays.asList("foo"); // global key only
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }
    @Test
    public void defaultSansGlobal_buildForIdentityOperation() {
      Identity id= new Identity();
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey(null).buildForIdentityOperation();
      expected = Arrays.<String>asList(); // exclude global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }
    @Test
    public void stuff_buildForIdentityOperation() {
      Identity id= new Identity();
      id.setAttribute(Identity.AWS_ACCOUNT, "fake1");
      id.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "fake2");
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey("foo").buildForIdentityOperation();
      expected = Arrays.asList("foo", "aws-account:fake1,Operation:MyService/OperationA", "http-remote-address:fake2,Operation:MyService/OperationA"); // with global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }
    @Test
    public void stuffSansGlobal_buildForIdentityOperation() {
      Identity id= new Identity();
      id.setAttribute(Identity.AWS_ACCOUNT, "fake1");
      id.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "fake2");
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey(null).buildForIdentityOperation();
      expected = Arrays.asList("aws-account:fake1,Operation:MyService/OperationA", "http-remote-address:fake2,Operation:MyService/OperationA"); // exclude global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }

    @Test
    public void default_buildForOperationIdentity() {
      Identity id= new Identity();
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey("foo").buildForOperationIdentity();
      expected = Arrays.asList("foo"); // with global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }
    @Test
    public void defaultSansGlobal_buildForOperationIdentity() {
      Identity id= new Identity();
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey(null).buildForOperationIdentity();
      expected = Arrays.<String>asList(); // exclude global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }
    @Test
    public void stuff_buildForOperationIdentity() {
      Identity id= new Identity();
      id.setAttribute(Identity.AWS_ACCOUNT, "fake1");
      id.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "fake2");
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey("foo").buildForOperationIdentity();
      expected = Arrays.asList("foo", "Operation:MyService/OperationA,aws-account:fake1", "Operation:MyService/OperationA,http-remote-address:fake2"); // with global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }
    @Test
    public void stuffSansGlobal_buildForOperationIdentity() {
      Identity id= new Identity();
      id.setAttribute(Identity.AWS_ACCOUNT, "fake1");
      id.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "fake2");
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder().withIdentities(IdentityKeyGenerator.DEFAULT_ATTRIBUTES);
      kg = kgb.withGlobalKey(null).buildForOperationIdentity();
      expected = Arrays.asList("Operation:MyService/OperationA,aws-account:fake1", "Operation:MyService/OperationA,http-remote-address:fake2"); // no global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }

    @Test
    public void default_buildForOperation() {
      Identity id= new Identity();
      id.setAttribute(Identity.AWS_ACCOUNT, "fake1");
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder();
      kg = kgb.withGlobalKey("foo").buildForOperation();
      expected = Arrays.asList("Operation:MyService/OperationA", "foo"); // with global key
      Assert.assertEquals(unsorted(expected), unsorted(kg.getKeys(ctx)));
    }
    @Test
    public void defaultSansGlobal_buildForOperation() {
      Identity id= new Identity();
      id.setAttribute(Identity.AWS_ACCOUNT, "fake1");
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder();
      kg = kgb.withGlobalKey(null).buildForOperation();
      expected = Arrays.asList("Operation:MyService/OperationA"); // exclude global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }
    @Test
    public void stuff_buildForOperation() {
      Identity id= new Identity();
      id.setAttribute(Identity.AWS_ACCOUNT, "fake1");
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder();
      kg = kgb.withGlobalKey("bar").buildForOperation();
      expected = Arrays.asList("bar", "Operation:MyService/OperationA"); // with global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }
    @Test
    public void stuffSansGlobal_buildForOperation() {
      Identity id= new Identity();
      id.setAttribute(Identity.AWS_ACCOUNT, "fake1");
      ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
      HandlerContext ctx = Mockito.mock(HandlerContext.class);
      Mockito.doReturn(id).when(ctx).getIdentity();
      Mockito.doReturn(sid).when(ctx).getServiceIdentity();

      kgb = new KeyGeneratorBuilder();
      kg = kgb.withGlobalKey(null).buildForOperation();
      expected = Arrays.asList("Operation:MyService/OperationA"); // exclude global key
      Assert.assertEquals(sorted(expected), sorted(kg.getKeys(ctx)));
    }

    @Test
    public void newBuilderForLoadShedHandler_then_buildForIdentity() {
    }
}
