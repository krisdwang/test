package com.amazon.coral.availability;

import java.util.ArrayList;

import com.amazon.coral.util.TextSequence;
import com.amazon.coral.model.Model;
import com.amazon.coral.service.Job;
import com.amazon.coral.service.ServiceHelper;

/** @Deprecated prefer com.amazon.coral.avail.key.OperationKeyGenerator */
@Deprecated
public class OperationKeyBuilder implements KeyBuilder {
  @Override
  public Iterable<CharSequence> getKeys(Job job) {
    ArrayList<CharSequence> keys = new ArrayList<CharSequence>(1);
    Model service = ServiceHelper.getServiceModel(job);
    if (service == null) {
      return keys;
    }
    Model.Id serviceId = service.getId();
    Model operation = ServiceHelper.getOperationModel(job);
    if (operation == null) {
      return keys;
    }
    Model.Id operationId = operation.getId();
    CharSequence key = TextSequence.newInstance("Operation:", serviceId.getName(), "/", operationId.getName());
    keys.add(key);
    return keys;
  }
}
