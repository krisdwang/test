package com.amazon.messaging.utils;

import lombok.Data;

import com.amazon.messaging.interfaces.Method;

@Data
public class MethodCall<Result, Arg, Excep extends Throwable> {
	private final Method<Result, Arg, Excep> method;
	private final Arg arg;

	public Result call() throws Excep {
		return method.call(arg);
	}
}
