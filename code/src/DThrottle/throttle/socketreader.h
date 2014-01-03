#ifndef _SOCKET_READER_H_
#define _SOCKET_READER_H_

/**
    Purpose: SocketReader lets you read line by line from a socket
    without worrying about partial/multiple line reads.

    Usage: Create a SocketReader by passing an open FLSocket to the 
    constructor, then register your callback with OnSocketReadable().
    In your callback, call lines() to get the buffer of unread lines and
    pop_front() as you process them. If OnSocketReadable() is called
    and lines().empty() is true, then you have hit EOF. 

    It is safe to destroy a SocketReader in it's callback *only if*
    lines().empty() == true when you are called back.

    Note: If the other end of the socket hangs up without \n terminating
    his last line, you will not be notified of the partial line.
  */

#include <list>

#if __GNUG__ < 3
#include <rope>
#else
#include <ext/rope>
#endif // __GNUG__ < 3

#include "flcore/FLObject.h"

// HACK - remove when pubsub/FLSocket is fixed
#warning Remove me when FLSocket supports AF_UNIX
#ifndef FLSocket
#define FLSocket FLSocket2
#endif


class FLSocket;
class FLNotification;

class SocketReader : public FLObject
{
private:
    P<FLSocket> _socket; 	// the socket
    std::list<std::string> _lines;	// buffer of lines read
    std::string _line;		// the most recent partial line
    P<FLCallBackBase> _cb;	// client's callback

    // our FLSocket callback
    void OnSocketReadable(const FLNotification *n);

public:
    // constructor
    SocketReader(FLSocket *socket);

    // access the buffer of lines
    std::list<std::string> &lines();

    // direct access to the socket (for replies)
    P<FLSocket> socket() { return _socket; }

    // use to register your callback
    void notifyReadable(const FLCallBackBase &cb);
};

#endif
