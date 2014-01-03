#include <iostream>
#include <vector>

#include "pubsub/FLDatagram.h"

#include "throttle/throttle.h"

extern "C" {
    int comm_test();
}

typedef std::vector< P<Throttle> > ThrottleVector;

void exchange_messages(ThrottleVector &vt, const double when)
{
    for (ThrottleVector::iterator i=vt.begin();i!=vt.end();i++)
    {
	Throttle *t = *i;
	P<FLDatagram> rpt = t->make_report(when);
	
	for (ThrottleVector::iterator j=vt.begin();j!=vt.end();j++)
	{
	    P<Throttle> other = *j;
	    if (i!=j)
	    {
		other->receive_report(rpt, when);
	    }
	}
    }
}

static void xfer_some()
{
    const double t = 1090012345;
    const char *tag = "john";
    const int burst = 10;
    const double rate = 1;

    ThrottleVector throttles;
    throttles.push_back( P<Throttle>(NEW, new Throttle(burst,rate)) );
    throttles.push_back( P<Throttle>(NEW, new Throttle(burst,rate)) );
 
    // hit both onlines for half of burst
    for (int i=0;i<(burst/2);i++)
    {
	assert(throttles[0]->check_request(tag, t+0)); 
	assert(throttles[1]->check_request(tag, t+0)); 
    }
    
    exchange_messages(throttles, t+0);

    assert(!throttles[0]->check_request(tag, t+0)); 
    assert(!throttles[1]->check_request(tag, t+0)); 
}

static void xfer_all()
{
    const double t = 1090012345;
    const char *tag = "john";
    const char *tag2 = "someoneelse";
    const int burst = 10;
    const double rate = 1;

    ThrottleVector throttles;
    throttles.push_back( P<Throttle>(NEW, new Throttle(burst,rate)) );
    throttles.push_back( P<Throttle>(NEW, new Throttle(burst,rate)) );
    
    for (int i=0;i<burst;i++)
    {
	assert(throttles[0]->check_request(tag, t+0)); 
    }
    assert(!throttles[0]->check_request(tag, t+0)); 

    exchange_messages(throttles, t+0);

    assert(throttles[0]->check_request(tag, t+1)); 
    assert(!throttles[0]->check_request(tag, t+1)); 
    assert(throttles[1]->check_request(tag, t+1)); 
    assert(!throttles[1]->check_request(tag, t+1)); 

    // ensure others are unaffected
    assert(throttles[0]->check_request(tag2, t+1)); 
    assert(throttles[1]->check_request(tag2, t+1)); 

}

static int rate_test(int burst, double allowed_rate, const double t_0,
	const double hit_rate, const double duration, const int n,
	double exchange_period)
{
    ThrottleVector throttles;
    for (int i=0;i<n;i++)
    {
	throttles.push_back( P<Throttle>(NEW,
		    new Throttle(burst,allowed_rate)) );
    }

    int hits = 0;
    const char *tag = "somebody infamous";
    double t = t_0;
    int i=0;
    double next_exchange_period = t_0 + exchange_period;

    while (t < t_0 + duration)
    {
	bool result = throttles[i++ % n]->check_request(tag, t);
	if (result) ++hits;

	t += 1/hit_rate;

	if (t >= next_exchange_period)
	{
	    exchange_messages(throttles, t);
	    next_exchange_period += exchange_period;
	}
    }

    double expected_hits;
    if (hit_rate >= allowed_rate)
    {
	expected_hits = (burst + allowed_rate*duration);
    }
    else
    {
	expected_hits = hit_rate * duration;
    }

    double ratio = ((double) hits)/expected_hits;
    bool is_ratio_ok = ratio > .95 && ratio < 1.10;
    if (!is_ratio_ok)
    {
	printf("%d ?= %f\n", hits, expected_hits);
	assert(is_ratio_ok);
    }

    return hits;
}

int comm_test()
{
    xfer_all();
    xfer_some();

    // 2 onlines
    rate_test(10, 1, 10000, 2, 1000, 2, 5); 

    // hit 20 onlines 20 r/s (allowed 1/s)
    rate_test(10, 1, 10000, 20, 1000, 20, 5); 

    // hit 20 onlines with 200 requests per second (allowed 1/s)
    rate_test(10, 1, 10000, 200, 1000, 20, 5); 

    return 0;
}

