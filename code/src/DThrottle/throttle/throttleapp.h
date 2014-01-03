#ifndef _THROTTLE_APP_H_
#define _THROTTLE_APP_H_

#include <hash_map>

#include "pubsub/FLApplication.h"
#include "pubsub/FLSubject.h"

// HACK - remove when pubsub/FLSocket is fixed
#warning Remove me when FLSocket supports AF_UNIX
#ifndef FLSocket
#define FLSocket FLSocket2
#endif

class SocketReader;
class Throttle;
class FLSocket;
class WPHTTPEmbeddedServer;

class ThrottleApp : public FLApplication
{
public:
    ThrottleApp(
	FLApplicationConfiguration* conf, FLEventLoop* eventLoop,
	FLApplication::Mode mode
    );

    void initialize();

    void OnEventLoopStarted(const FLNotification *n);

    // Called when a new client connects
    void OnSocketConnect(const FLNotification *n);

    // Called when a client makes a throttling request
    void OnReadable(const FLNotification *n);

    // Called when we should multicast out a throttling report
    void OnTimer(const FLNotification *n);

    // Called when we recieve a throttling report from another instance
    void OnMessageReceived(const FLNotification *n);

private:
    P<Throttle> _throttle;		// Implements the leaky bucket
    FLSubject _subject;			// The subject for Throttle reports
    P<FLTimerEventSpecifier> _timer;	// Used to periodically multicast reports
    P<FLSocket> _server_socket;		// Listen for client connections
    P<FLSocket> _server_socket_unix;	// ... also in the UNIX domain
    
    P<FLConfigValue> _never_throttle;	// Are we enabled?
    P<FLConfigValue> _radio_silence;	// Talk to other instances?

    typedef std::hash_map<intptr_t, P<SocketReader> > HashType;
    HashType _client_readers;		// Keep track of client connections

    bool check_request(const char *tag);
    void process_request(const char *tag, FLSocket *replySocket);


};

#endif
