#ifndef _THROTTLE_H_
#define _THROTTLE_H_

#include <sys/time.h>

#include "flcore/FLRetainable.h"

#include "trie.h"

#if __GNUG__ < 3
#include <rope>
#else
#include <ext/rope>
#endif // __GNUG__ < 3

#include <hash_map>

class FLDatagram;
 
/**
    Purpose: An implementation of the leaky bucket throtting algorithm.

    Usage: Clients are tracked and throttled by a string tag. Call hit()
    to register each request -- it will return false if and only if
    the request should be throttled. Also, receive_report() gives you the 
    capability to register client requests that were allowed but not under
    our direct throttling control (for example, served by another machine).
    Finally, make_report() constructs a datagram of tag, hitcount pairs
    which include the number of requests allowed since the last time
    make_report() was called. The FLDatagram produced is useful for sending
    to other instances of Throttle which are managing the same collective 
    resource.

  */
class Throttle : public FLRetainable
{
public:
    /**
        Throttling parameters:

	burst: The capacity of the leaky bucket.
	rate: The rate (in requests per second) at which the bucket is 
	    refilled
       */
    struct Parameters 
    {
	int burst;
	double rate;
	bool whitelisted;

	Parameters() : burst(0), rate(0), whitelisted(false) {}
	Parameters(int b, double r, bool w = false) :
	    burst(b), rate(r), whitelisted(w) {}
    };

    /**
      Construct a Throttle object with the specified throttling parameters
      as default. ie: if no rule matches the throttling tag (see add_rule)
      the tag will be throttled according to these default parameters.
       */
    Throttle(const int burst, const double rate);

    /**
      Registers a client request at time 't'.
      Returns false iff the request should be throttled.
      */
    bool check_request(const char *tag, const double t);

    /**
      Adds an entry to a whitelist.
      If any entry on the whitelist is a prefix of a throttling tag, that tag
      will never be throttled.
      */
    void whitelist(const char *tag_prefix);

    /**
      Configures throttling tags that match the specified prefix
      to be throttled according to the specified parameters.

      If throttling tag matches more than one rule, the most specific rule
      applies. For example if:

	  add_rule("1-", p0);
	  add_rule("1-206-", p1);
	  add_rule("1-206-456", p2);

	  // tag 1-206-456-1234 will be throttled at p2
	  // tag 1-206-789-0123 will be throttled at p1
	  // tag 1-415-789-0123 will be throttled at p0

      */
    void add_rule(const char *tag_prefix, const Parameters &p);

    /**
      Creates, at time t, an FLDatagram suitable for sending to other Throttle
      instances.
      */
    NewP<FLDatagram> make_report(const double t);

    /**
      Process an FLDatagram received from another Throttle instance at time t.
      (For collective throttling)
      */
    void receive_report(FLDatagram *dg, const double t);

    /**
      For debugging
      */
    void dump_state(std::ostream &out);

private:
    // Store one of these for each client to keep track of their "bucket"
    class usage_record
    {
    private:
	double _tokens; // how many tokens in the bucket?
	double _last_update; // when the bucket was last updated?
	int _unreported_hits; // how many hit()s since last report?
    public:
	usage_record(int tokens, double now) :
	    _tokens(tokens), _last_update(now), _unreported_hits(0) {}

	// allow/throttle a request by this client at time t
	bool check_request(const int burst, const double rate,
		const double t);

	double get_tokens(const int burst, const double rate,
		const double t);

	void record_external_hits(int count) { _tokens -= count; }

	// retreive and reset the hits counter
	int get_unreported_hits() const { return _unreported_hits; }
	void reset_unreported_hits() { _unreported_hits = 0; }

	friend std::ostream &operator<<(std::ostream &s, const Throttle::usage_record &r);
    };
    friend std::ostream &operator<<(std::ostream &s, const Throttle::usage_record &r);

    typedef hash_map<crope, usage_record> record_map_t;
    record_map_t _map;

    typedef trie<Parameters> trie_t;
    trie_t _rules;

    /**
      Registers, at time t, client requests that were satisfied without 
      our consent.
      */
    void external_hit(const char *tag, int count, const double t);

    /**
      Retrieves/Creates the usage record for "tag" at time t
      */
    usage_record &get_record(const crope &tag, const double t);
    
    Parameters &get_params(const crope &tag);
};

#endif
