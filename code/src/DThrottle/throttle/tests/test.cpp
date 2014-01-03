#include <iostream>

#include "throttle/throttle.h"

extern "C" {
    int test();
}

static void basic_test()
{
    const double t = 1090026837;
    const char *tag = "john";
    Throttle throttle( /* burst = */ 1, /* rate = */ 1);

    // succeeds because bucket is full (1)
    assert(throttle.check_request(tag, t+0)); 

    // fails because 0 tokens
    assert(!throttle.check_request(tag, t+0)); 
}

static void more_tests()
{
    const double t = 1090026999;
    const char *tag = "john";
    const char *tag2 = "someoneelse";
    const int burst = 10;
    const double rate = 1;
    Throttle throttle( burst, rate);
    
    for (int i=0;i<burst;i++)
    {
	assert(throttle.check_request(tag, t+0)); 
    }
    assert(!throttle.check_request(tag, t+0)); 

    // time passes
    assert(throttle.check_request(tag, t+1)); 
    assert(!throttle.check_request(tag, t+1)); 

    // ensure others are unaffected
    assert(throttle.check_request(tag2, t+1)); 

    // four seconds later, we're allowed exactly four more requests
    for (int i=0;i<4;i++) assert(throttle.check_request(tag, t+5)); 
    assert(!throttle.check_request(tag, t+5)); 

    // make sure bucket doesn't overflow with time
    const double much_later = t + 2*burst / rate;
    for (int i=0;i<burst;i++) assert(throttle.check_request(tag, much_later));
    assert(!throttle.check_request(tag, much_later)); 
}

static int count_hits(Throttle &throttle, const char *tag,
	const double t_0, const double hit_rate, const double duration)
{
    int hits = 0;
    double t = t_0;

    while (t < t_0 + duration)
    {
	bool result = throttle.check_request(tag, t);
	if (result) ++hits;

	t += 1/hit_rate;
    }
    
    return hits;
}

static int rate_test(
	Throttle &throttle, const char *tag, const double hit_rate, 
	const double expected_rate, const int expected_burst,
	const double t_0, const double duration)
{
    //printf("rate_test: tag='%s', hit_rate=%f, dur=%f\n", tag, hit_rate, duration);
    int hits = count_hits(throttle, tag, t_0, hit_rate, duration);

    // calc expected hits
    double expected_hits;
    if (hit_rate >= expected_rate)
    {
	expected_hits = (expected_burst + expected_rate*duration);
    }
    else
    {
	expected_hits = hit_rate * duration;
    }

    double ratio = ((double) hits)/expected_hits;
    bool is_ratio_ok = ratio > .95 && ratio < 1.05;
    //printf("\t%d hits ?= expected_hits %f\n", hits, expected_hits);
    if (!is_ratio_ok)
    {
	assert(is_ratio_ok);
    }

    return hits;
}


static int simple_rate_test(int burst, double allowed_rate, const double t_0,
	const double hit_rate, const double duration)
{
    Throttle throttle(burst, allowed_rate);
    const char *tag = "somebody famous";

    return rate_test(throttle, tag, hit_rate, allowed_rate, burst, t_0, duration);
}

static void per_client_config_test()
{
    Throttle t(1,6);
    t.add_rule("192.", Throttle::Parameters(1, 10));
    t.add_rule("172.", Throttle::Parameters(1, 20));
    t.add_rule("172.1.1.9", Throttle::Parameters(1, 30, /*whitelist=*/ true));
    t.add_rule("10.12.", Throttle::Parameters(1, 30));

    // expected 10/s
    rate_test(t, "192.168.1.1", 5, 10, 1, 1, 10000);
    rate_test(t, "192.168.1.2", 15, 10, 1, 100000, 10000);

    // expected 20/s
    rate_test(t, "172.12.1.1", 15, 20, 1, 200000, 10000);
    rate_test(t, "172.12.1.2", 25, 20, 1, 300000, 10000);

    // test whitelisted
    rate_test(t, "172.1.1.9", 15, 15, 1, 400000, 10000);
    rate_test(t, "172.1.1.9", 25, 25, 1, 500000, 10000);

    // expected 30/s
    rate_test(t, "10.12.1.1", 25, 30, 1, 600000, 10000);
    rate_test(t, "10.12.1.2", 30, 30, 1, 700000, 10000);

    // this guy doesn't match a rule so he gets the default 6/s
    rate_test(t, "204.112.1.1", 4, 6, 1, 800000, 10000);
    rate_test(t, "204.112.1.2", 8, 6, 1, 900000, 10000);
}

int test()
{
    basic_test();

    more_tests();

    simple_rate_test(20, 4.0/3.0, 12342143, 8.0/3.0, 10000);
    simple_rate_test(20, 4.0/3.0, 21893782, 4.0/3.0, 10000);
    simple_rate_test(10, .5, 128372, 10, 10000);
    simple_rate_test(10, 2.5, 4321789, 2, 10000);

    per_client_config_test();

    return 0;
}

