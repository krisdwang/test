// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import java.util.ArrayList;

import com.amazon.coral.util.TextSequence;
import com.amazon.coral.service.HandlerContext;
import com.amazon.coral.service.ServiceIdentity;

public class OperationKeyGenerator extends KeyGenerator {
  @Override
  public Iterable<CharSequence> getKeys(HandlerContext ctx) {
    ArrayList<CharSequence> keys = new ArrayList<CharSequence>(1);
    ServiceIdentity si = ctx.getServiceIdentity();
    CharSequence key = TextSequence.newInstance("Operation:", si.getServiceId().getName(), "/", si.getOperationId().getName());
    keys.add(key);
    return keys;
  }
}
