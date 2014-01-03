#include "pubsub/FLApplication.h"
#include "pubsub/FLNotificationCenter.h"
#include "pubsub/FLCallBack.h"
#include "pubsub/FLDatagram.h"
#include "pubsub/FLSubject.h"
#include "pubsub/FLNotification.h"
#include "pubsub/FLEventSpecifier.h"
#include "pubsub/FLSignalManager.h"
// pubsub/FLSocket does not support the AF_UNIX family that we need
// When/If this support is added, we can use the pubsub version
// #include "pubsub/FLSocket.h"
#include "throttle/FLSocket2.h"

#include "flcore/FLConfigValue.h"
#include "flcore/FLDictionary.h"
#include "flcore/FLBuffer.h"
#include "flcore/FLLog.h"

#include "throttle/throttle.h"
#include "throttle/throttleapp.h"
#include "throttle/socketreader.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

static double now()
{
    // get the current time as a double
    struct timeval now_timeval;
    gettimeofday(&now_timeval, NULL);
    return (double) now_timeval.tv_sec + 
	(double) now_timeval.tv_usec / 1000000.0L;
}

ThrottleApp::ThrottleApp(
    FLApplicationConfiguration* conf, FLEventLoop* eventLoop,
    FLApplication::Mode mode
) :
    FLApplication(conf, eventLoop, mode), 
    _subject("DThrottle.Heartbeat")
{
    /**
      @config DThrottle.burst (int):
      @default 10
      The leaky bucket capacity, in requests.
     */
    int burst = 10;
    FLConfigKey burstKey("DThrottle.burst");
    FLAppConfig->findValue(burstKey, burst);

    /**
      @config DThrottle.rate (double):
      @default 1.0
      Long term average rate, in requests per second.
     */
    double rate = 1;
    FLConfigKey rateKey("DThrottle.rate");
    FLAppConfig->findValue(rateKey, rate);

    _throttle.NEW(new Throttle(burst, rate));

    /**
      @config DThrottle.neverThrottle (bool):
      @default false
      If true, we will respond OK to all throttling queries..
      i.e. we are disabled.
     */
    FLConfigKey enabledKey("DThrottle.neverThrottle");
    _never_throttle.NEW(new FLConfigValue(&enabledKey, false));

    /**
      @config DThrottle.radioSilence (bool):
      @default false
      If true, we will never publish stats, nor will we process messages
      from other instances. i.e. "collective throttling" is disabled.
     */
    FLConfigKey radioSilenceKey("DThrottle.radioSilence");
    _radio_silence.NEW(new FLConfigValue(&radioSilenceKey, false));

}

void ThrottleApp::initialize()
{
    this->FLApplication::initialize();

    DNC().addObserver(
	    SSEventLoopStarted(),
	    FLCallBack<ThrottleApp>
	    (this, &ThrottleApp::OnEventLoopStarted)
	    );
    
    // read whitelist from config
    P<const FLValueArray> cfg_whitelist;

    /**
      @config DThrottle.whitelist (vector<string>):
      @default empty
      Hosts that match one of these prefixes will never be throttled.
     */
    FLConfigKey whitelistKey("DThrottle.whitelist");
    if (FLAppConfig->findValue(whitelistKey, cfg_whitelist))
    {
	for (
	    FLValueArray::const_iterator i = cfg_whitelist->begin();
	    i != cfg_whitelist->end();
	    ++i)
	{
	    std::string tag;
	    FLValue *value = (*i);
	    if (value->get(tag))
	    {
		_throttle->whitelist(tag.c_str());
	    }
	}
    }

    /**
      @config DThrottle.rules (dictionary<vector<int>>):
      @default empty
      entires are :
	tag_prefix => (burst, rate) 
     */
    P<const FLValueDictionary> cfg_rules;
    FLConfigKey rulesKey("DThrottle.rules");
    if (FLAppConfig->findValue(rulesKey, cfg_rules))
    {
	for (
	    FLDictionaryIterator i = cfg_rules->begin();
	    i != cfg_rules->end();
	    ++i)
	{
	    P<const FLValueArray> params;
	    if (!i.value().get(params))
	    {
		FLLogError(FLAppConfig->getApplicationName(),
			"rule for key %s is not an FLDictionary", i.key());
		continue;
	    }
	    if (params->size() != 2)
	    {
		FLLogError(FLAppConfig->getApplicationName(),
			"rule for key %s has %d params != 2",
			i.key(), params->size());
		continue;
	    }

	    Throttle::Parameters p;
	    if (! (*params)[0]->get(p.burst, true))
	    {
		FLLogError(FLAppConfig->getApplicationName(),
			"can't parse burst value for rule %s",
			i.key());
		continue;
	    }
	    if (! (*params)[1]->get(p.rate, true))
	    {
		FLLogError(FLAppConfig->getApplicationName(),
			"can't parse rate value for rule %s",
			i.key());
		continue;
	    }

	    FLLogError(FLAppConfig->getApplicationName(),
		    "adding rule for key %s", i.key());
	    _throttle->add_rule(i.key(), p);
	}
    }
}

void ThrottleApp::OnEventLoopStarted(const FLNotification *n)
{
    // DThrottle always ignores SIGPIPE.  Since each transaction with its clients is a single
    // request-reply, there is nothing useful it can do to recover, and dying won't help.
    FLSignalManager::instance().setSignalBehavior(FLSignalManager::Signal::PIPE, 
                                                  FLSignalManager::SigAction::IGNORE);
    
    /**
      @config DThrottle.interval (double):
      @default 5.0
      Heartbeat interval in seconds. Lower numbers yield more frequent, 
      but smaller messages. Higher numbers yield larger, infrequent 
      publishes.
     */
    double interval = 5.0;
    FLConfigKey intervalKey("DThrottle.interval");
    FLAppConfig->findValue(intervalKey, interval, 1, 3600);

    // start timer
    _timer.NEW(new FLTimerEventSpecifier(interval));

    DNC().addObserver(
	_timer, 
	FLCallBack<ThrottleApp>(this, &ThrottleApp::OnTimer)
    );
    _timer->start();
    
    // listen for others
    DNC().addObserver(
	    _subject,
	    FLCallBack<ThrottleApp>
	    (this, &ThrottleApp::OnMessageReceived)
	    );

    /**
      @config DThrottle.port (int):
      @default 6969
      TCP Port to listen on for throttling queries.
     */
    int port = 6969; // default
    FLConfigKey portKey("DThrottle.port");
    FLAppConfig->findValue(portKey, port, 0, 65535);

    // listen for clients
    FLCallBack<ThrottleApp> cb(this, &ThrottleApp::OnSocketConnect);
    _server_socket.NEW(new FLSocket(port));
    _server_socket->acceptConnection(cb);

    /**
      @config DThrottle.socketpath (int):
      @default "/tmp/dthrottle.sock"
      UNIX domain socket address to listen on for throttling queries.
     */
    std::string socket_path("/tmp/dthrottle.sock");
    FLConfigKey socketpathKey("DThrottle.socketpath");
    FLAppConfig->findValue(socketpathKey, socket_path);

    // listen for clients in the UNIX domain
    FLCallBack<ThrottleApp> cb_unix(this, &ThrottleApp::OnSocketConnect);
    unlink(socket_path.c_str());
    _server_socket_unix.NEW(
	new FLSocket(socket_path.c_str(), SOCK_STREAM, AF_LOCAL, 0)
    );
    _server_socket_unix->acceptConnection(cb_unix);

    // setup the socket file with all permissions
    if (chmod(socket_path.c_str(), 0777) < 0)
    {
	FLLogError(FLAppConfig->getApplicationName(), "chmod: %s",
		strerror(errno));
    }
}

void ThrottleApp::OnSocketConnect(const FLNotification *n)
{
    P<FLSocket> sock;
    if (n->getObject(sock))
    {
	P<SocketReader> reader(NEW, new SocketReader(sock));
	reader->notifyReadable(
	    FLCallBack<ThrottleApp>
	    (this, &ThrottleApp::OnReadable)
	);

	_client_readers[(intptr_t)reader.getPointer()] = reader;
    }
}

void ThrottleApp::OnTimer(const FLNotification *n)
{
    P<FLDatagram> dg = _throttle->make_report(now());

    if (!_radio_silence->getBool())
    {
	// publish the datagram 
	DNC().postNotification(_subject, dg);
    }

    _timer->start();
}

void ThrottleApp::OnMessageReceived(const FLNotification *n)
{
    FLDatagram *dg = n->getDatagram();
    if (dg && !n->isLocal() && !_radio_silence->getBool())
    {
	_throttle->receive_report(dg, now());
    }
}


bool ThrottleApp::check_request(const char *tag)
{
    if (_never_throttle->getBool())
    {
	return true;
    }
    else
    {
	return _throttle->check_request(tag, now());
    }
}

void ThrottleApp::process_request(const char *tag, FLSocket *replySocket)
{
    bool result = check_request(tag);
    const char * response;

    if (result == true)
    {
	response = "OK\n";
    }
    else
    {
	response = "NO\n";
	FLLogInform(FLAppConfig->getApplicationName(), "%s %s",
		response, tag);
    }

    try {
	replySocket->write(response, 3);
    }
    catch (const FLException &e)
    {
	// ignore exception
	// writing the reply may fail if the client has gone away
    }
}

void ThrottleApp::OnReadable(const FLNotification *n)
{
    P<SocketReader> reader;
    if (n->getObject(reader))
    {
	std::list<std::string> &lines = reader->lines();
	if (lines.empty())
	{
	    // EOF - remove socket reader
	    intptr_t key = (intptr_t)reader.getPointer();
	    HashType::iterator i = _client_readers.find(key);
	    _client_readers.erase(i);

	    return;
	}

	while (!lines.empty())
	{
	    const std::string &line = reader->lines().front();
	    process_request(line.c_str(), reader->socket());
	    reader->lines().pop_front();
   	}
    }
}

int main(int argc, char **argv, char **envp)
{
    FLApplicationInstantiator<ThrottleApp> app(NULL, argc, argv, envp);

    return 0;
}
