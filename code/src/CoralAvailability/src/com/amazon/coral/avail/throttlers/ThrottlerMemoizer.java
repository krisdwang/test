// vim: noet ts=4 sts=4 sw=4 tw=0
package com.amazon.coral.avail.throttlers;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazon.coral.service.HandlerContext;
import com.amazon.coral.throttle.api.Throttler;

public class ThrottlerMemoizer extends ThrottlerRetriever {
	private static final AtomicInteger unique = new AtomicInteger(8675309);

	static class ThrottlerMap {
		HashMap<String, Throttler> throttlers = new HashMap<String, Throttler>();
		Throttler get(String key) {
			return throttlers.get(key);
		}
		void put(String key, Throttler val) {
			throttlers.put(key,  val);
		}
	}
	private final String key;
	private final static String MEMOIZER = "MEMOIZER";

	/** Warning: This is strictly to support the old behavior of a single, global MEMOIZER. */
	public ThrottlerMemoizer(Throttler throttler) {
		this(throttler, "NEW_GLOBAL_MEMOIZER"+unique.getAndIncrement());
	}
	public ThrottlerMemoizer(Throttler throttler, String key) {
		super(throttler);
		this.key = key;
	}
	@Override
	public Throttler getThrottler(HandlerContext ctx) {
		ThrottlerMap m = ctx.getUserState(ThrottlerMemoizer.class, MEMOIZER);
		if (null == m) {
			m = new ThrottlerMap();
			ctx.setUserState(ThrottlerMemoizer.class, MEMOIZER, m);
		}
		Throttler t = m.get(key);
		if (t == null) {
			t = new MemoizedThrottler(this.throttler);
			m.put(key, t);
		}
		return t;
	}
}
