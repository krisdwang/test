#include <stdio.h>
#include <sys/time.h>

#include <iostream>
#include <sstream>
#include <hash_map>

#include "pubsub/FLApplication.h"
#include "pubsub/FLNotificationCenter.h"
#include "pubsub/FLCallBack.h"
#include "pubsub/FLDatagram.h"
#include "pubsub/FLSubject.h"
#include "pubsub/FLNotification.h"
#include "pubsub/FLEventSpecifier.h"
#include "pubsub/FLSocket.h"
#include "flcore/FLBuffer.h"
#include "flcore/FLAssert.h"

#include "throttle/throttle.h"

static char *rcsid="$Id: //brazil/src/appgroup/webservices/apps/DThrottle/mainline/throttle/throttle.cpp#10 $";
static void *const use_rcsid = (void *) ( (void)&rcsid, 0);

using namespace std;

double Throttle::usage_record::get_tokens(
    const int burst, const double rate, const double t
)
{
    // t == 0 is special and used internally
    FLAssert(t > 0);

    // how many tokens have flown into the bucket since the last time?
    if (_last_update)
    {
	double sec_elapsed = t - _last_update;
	_tokens += sec_elapsed * rate ;

	// enforce bucket capacity
	if (_tokens > burst) _tokens = burst;
    }

    // update "last time"
    _last_update = t;

    return _tokens;
}

bool Throttle::usage_record::check_request(
    const int burst, const double rate, const double t
)
{
    // consume one token for this request
    if (get_tokens(burst, rate, t) > 0)
    {
	_tokens--;
	_unreported_hits++; // count number of hits for make_report()
	return true;
    }
    return false;
}

Throttle::Throttle(const int burst, const double rate)
{
    // setup the default rule to be p
    Throttle::Parameters p(burst, rate, /* whitelist = */ false);
    _rules.insert("", p);
}

bool Throttle::check_request(const char *tag_cstr, const double t)
{
    const crope tag(tag_cstr);

    // find throttling parameters for this tag
    const Throttle::Parameters &p = get_params(tag);

    if (p.whitelisted) {
        FLLogDebug("Throttle::check_request", "%s permitted: whitelisted", tag_cstr);
        return true;
    }

    // find client's record
    usage_record &r = get_record(tag, t);

    bool success = r.check_request(p.burst, p.rate, t);

    if (FLLog::instance().isLevelEnabled(FLLog::Debug) ) {
        std::stringstream dumpRecord;
        dumpRecord << tag_cstr << (success ? " permitted" : " rejected") << ": " << r;
        FLLogDebug("Throttle::check_request", dumpRecord.str());
    }

    return success;
}

void Throttle::whitelist(const char *tag_prefix)
{
    _rules.insert(tag_prefix, Parameters(0, 0, /* whitelisted = */ true));
}

void Throttle::add_rule(const char *tag_prefix, const Parameters &p)
{
    _rules.insert(tag_prefix, p);
}

void Throttle::external_hit(const char *tag, int count, double t)
{
   usage_record &r = get_record(crope(tag), t);
   r.record_external_hits(count);
}

// receive and process a report from another Throttle instance
void Throttle::receive_report(FLDatagram *dg, double t)
{
    // the report format is (key = tag, value = hit_count_since_last_time)
    for (FLDictionaryIterator i = dg->begin(); i != dg->end(); ++i)
    {
	const char *tag = i.key();
	int external_hits;
	if (i.value().get(external_hits))
	{
	    external_hit(tag, external_hits, t);
	}
    }
}

// produce a report of all hits since "last time" suitable for sending to
// other Throttle instances
NewP<FLDatagram> Throttle::make_report(const double t)
{
    P<FLDatagram> dg(NEW, new FLDatagram());

    record_map_t::iterator i=_map.begin();
    while (i!=_map.end())
    {
	const crope &tag = (*i).first;
	usage_record &r = (*i).second;

	int hits = r.get_unreported_hits();
	if (hits > 0) // only report non-zero hit_counts
	{
	    dg->insertKeyAndValue(tag.c_str(), hits);
	}
	r.reset_unreported_hits();

	// take this oportunity to erase unneeded records
	const Parameters &p = get_params(tag);
	if (r.get_tokens(p.burst, p.rate, t) >= p.burst)
	{
	    // if the bucket is full, there's no point in keeping
	    // this record around anymore
	    _map.erase(i++);
	    continue; // for safety, since (tag,r) are now invalid
	}

	++i;
    }
    
    return NewP<FLDatagram>(dg);
}

Throttle::Parameters &Throttle::get_params(const crope &tag)
{
    return *_rules.lookup(tag.c_str());
}

Throttle::usage_record &Throttle::get_record(const crope &tag, double t)
{
    record_map_t::iterator i = _map.find(tag);
    if (i != _map.end())
    {
	return (*i).second;
    }

    // the record doesn't exist -- create it with some initial number of 
    // tokens in it's bucket
    const Parameters &p = get_params(tag);
    int initial_tokens = p.burst;

    return (*((_map.insert(record_map_t::value_type(tag, usage_record(initial_tokens, t)))).first)).second;
}

void Throttle::dump_state(std::ostream &out)
{
    out.setf( ios_base::hex );
    out.setf( ios_base::showbase );
    out << "Throttle " << ((intptr_t) this) << ":" << endl;
    out.unsetf( ios_base::hex );
    out.unsetf( ios_base::showbase );

    for (record_map_t::iterator i=_map.begin();i!=_map.end();i++)
    {
	const crope &tag = (*i).first;
	usage_record &r = (*i).second;

	out << tag << "\t\t" << r << endl;
    }
    out << endl;
}

std::ostream &operator<<(std::ostream &s, const Throttle::usage_record &r)
{
    return s << r._tokens << " tokens, " << 
	"last updated: " << r._last_update << ", " <<
	r._unreported_hits << " unreported hits.";
}
