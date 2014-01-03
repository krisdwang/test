#include "flcore/FLBuffer.h"
#include "pubsub/FLCallBack.h"
#include "pubsub/FLNotification.h"

// pubsub/FLSocket does not support the AF_UNIX family that we need
// When/If this support is added, we can use the pubsub version
// #include "pubsub/FLSocket.h"
#include "throttle/FLSocket2.h"

#include "throttle/socketreader.h"

SocketReader::SocketReader(FLSocket *socket) :
    _socket(socket)
{
    _socket->notifyReadable(
	FLCallBack<SocketReader>(
	    this, &SocketReader::OnSocketReadable
	)
    );
}

void SocketReader::OnSocketReadable(const FLNotification *n)
{
    try {
	// note: a read() of 0 (EOF) throws an exception 
	P<FLBuffer> chunk = _socket->read(-1, NULL);
	const char *chars = chunk->chars();
	if (chars)
	{
	    while (*chars)
	    {
		if (*chars != '\n')
		{
		    _line += *chars;
		}
		else
		{
		    _lines.push_back(_line);
		    _line.clear();
		}
		chars++;
	    }
	}
    }
    catch (FLException e)
    {
	_socket->close();
	_socket = NULL;
    }

    // notify client of new lines until they are consumed
    while (!_lines.empty())
    {
	FLNotification n(FLSubject::SSNull, this, this);
	_cb->invoke(&n);
    }
    
    // notify client of EOF 
    if (_socket == NULL)
    {
	FLNotification n(FLSubject::SSNull, this, this);
	_cb->invoke(&n);
    }

    // WARNING: client may have destroyed us in the previous callbacks
    // so "this" may now be invalid! Do not touch members after this point!
}

std::list<std::string> &SocketReader::lines()
{
    return _lines;
}

void SocketReader::notifyReadable(const FLCallBackBase &cb)
{
    _cb = cb.clone();
}
