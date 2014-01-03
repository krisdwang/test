static const char* const _FLSocket_cpp_rcs = __FILE__

"$Id: //brazil/src/appgroup/webservices/apps/DThrottle/mainline/throttle/FLSocket2.cpp#2 $";

/*
File:	FLSocket.cpp
Author:	Jay Sipelstein (sipelste@susq.com)
Date:	Wed Sep 23 11:07:15 1998

Copyright (c) 1998 Susquehanna Partners, G.P. All rights reserved.
 
*/


#if defined(_WIN32)
#include <windows.h>
#define EINPROGRESS WSAEINPROGRESS
typedef int ssize_t;
#else 
#include <sys/fcntl.h>
#include <netdb.h>
#include <strings.h>
#endif
#include <errno.h>
#include "flcore/FLAssert.h"
#include "flcore/FLString.h"
#include "flcore/FLLog.h"
#include "pubsub/FLEventSpecifier.h"
#include "pubsub/FLNotification.h"
#include "pubsub/FLNotificationCenter.h"
#include "flcore/FLPosixImpl.h"
#include "flcore/FLBuffer.h"

#include "throttle/FLSocket2.h"

// TODO: LOTS of bulletproofing!

DEFINE_CLASS(FLSocket, FLObject);
   

FLSocket::~FLSocket()
{
  close();
}


FLSocket::FLSocket(int fd, const sockaddr_in& socketStruct)
  : fd_(fd), socketStruct_(socketStruct), initialized_(false)
{
  FLLogVerbose("FLSocket", "constructing fd %u", fd_);
}

FLSocket::FLSocket(const char *path,
		   int		type,
		   int		family,
		   int		protocol)
  : fd_(-1), path_(path), type_(type), family_(family), protocol_(protocol),
    initialized_(false)
{
  FLLogVerbose("FLSocket", "constructing fd %d path %s", fd_, path);
}


FLSocket::FLSocket(in_port_t	portNum,
		   int		type,
		   int		family,
		   int		protocol)
  : fd_(-1), port_(portNum), type_(type), family_(family), protocol_(protocol),
    initialized_(false)
{
  FLLogVerbose("FLSocket", "constructing fd %d port %d", fd_, (int)portNum);
}

// The following constructor must be fleshed out if this class is to be either or
// Codable or instantiated by the class manager:

FLSocket::FLSocket(FLClassFlag)
  : initialized_(false)
{
  throw FLException("Incomplete FLClassFlag constructor for FLSocket");
}   



// superclass method implementations  **************************************

FLString FLSocket::print() const
{
  FLString result = FLObject::print();
  // result += "really useful debugging stuff"; 
  return result;
}


/* virtual */
bool 
FLSocket::isValid() const
{
  try {
    readable();
    return true;
  }
  catch(...) {
    return false;
  }
}

// end superclass method implementations  **********************************

// client uses this to tell me what to call back when I am readable.
void FLSocket::notifyReadable(const FLCallBackBase &cb)
{
  cbClient_ = cb.clone();
}

void FLSocket::initReadableCallback_(void (FLSocket::*cb)(const FLNotification*))
{
  socketIOEventSpecifier_.NEW
    (new FLIoEventSpecifier(fd_, FLIoEventSpecifier::Readable));
  
  // call *me* back when I become readable, I'll forward myself to whomever
  // wants me
  DNC().addObserver(socketIOEventSpecifier_,
		    FLCallBack<FLSocket>(this, cb));
  socketIOEventSpecifier_->start();
}


// generally, I've got data, let somebody know
void FLSocket::readable_(const FLNotification * notification)
{
  // TODO: really need to check that I *am* readable, not here because of some
  // error condition ...
  FLAssertAlways(notification != NULL);

  if( cbClient_ )
    {
      P<FLNotification> note(NEW, new FLNotification(FLSubject::SSNull, this, this));
      cbClient_->invoke(note);
    }
  else {
    throw FLException("FLSocket::readable_(): "
		      "did you forget to call notifyReadable on a socket?");
  }
}

bool FLSocket::init_()
{
  // "C" initialization

  fd_ = ::socket(family_, type_, protocol_);  
  if( fd_ < 0 )
    {
      FLLogError("FLSocket", "Opening socket in Socket::init_(): %s",
		 strerror(errno));
      return false;
    }

  FLLogVerbose("FLSocket", "initializing socket fd %u ...", fd_);
  
  int length;
  struct sockaddr *mysockaddr = NULL;
  if (family_ == AF_UNIX)
  {
      if (path_.length() >= sizeof(socketStructUnix_.sun_path))
      {
	  FLLogError("FLSocket", "Path too long!");
	  return false;
      }
      strncpy(socketStructUnix_.sun_path, path_.c_str(),
	      sizeof(socketStructUnix_.sun_path)); 

      socketStructUnix_.sun_family = family_;
      length = sizeof(socketStructUnix_);
      mysockaddr = (struct sockaddr*) &socketStructUnix_;
  }
  else // the assumption that we want sockaddr_in is from old code, not me
  {
      socketStruct_.sin_family = family_;
      socketStruct_.sin_addr.s_addr = INADDR_ANY;
      socketStruct_.sin_port = htons(port_);
      length = sizeof(socketStruct_);
      mysockaddr = (struct sockaddr*) &socketStruct_;
  }
      
  int val = 1;
  ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, (const char*)&val, sizeof(int));
  if( bind(fd_, mysockaddr, length)
       < 0 )
    {
      FLLogError("FLSocket",
		 "Binding socket (fd %d, port %d) in Socket::init_(): %s",
		 fd_, port_, strerror(errno));
      return false;
    }
      
  initialized_ = true;
  return true;
}  

bool FLSocket::acceptConnection(FLCallBackBase &cb,
				int maxClientBacklog)
{
  // the following initialization code "should" be in the constructor, but that
  // would force us to have two different kinds of sockets depending on whether
  // the socket is going to be used to receive or initiate the connection.
  
  init_();

  if( listen(fd_, maxClientBacklog) < 0 )
    {
      FLLogError("FLSocket", "listen() in Socket::acceptConnection(): ",
		 strerror(errno));
      return false;
    }

  // all the "C" level setup is done.  Now, register a callback to myself
  // to handle someone connecting to me.
  // note, there is a known race condition here, but it's pretty tiny, and if
  // you register the callback before listening, you GET a "bogus" callback
  // under linux 2.4 that you have to hack around.  this seems like a better
  // solution. (paul@ Sat Dec 22 13:36:37 PST 2001)
  
  initReadableCallback_(&FLSocket::doAccept_);
  cbClient_ = cb.clone();
      
  return true;
}


void FLSocket::doAccept_(const FLNotification *notification)
{
  P<const FLIoEventSpecifier> ioEvent(DOWNCAST, notification->getSender());

  struct sockaddr	fromAddr;
  struct sockaddr_in   *fromAddrIn = (struct sockaddr_in *) &fromAddr; // IP
  socklen_t		fromSize = sizeof (*fromAddrIn);

  int listenerFd = ioEvent->getDescriptor();

  FLLogVerbose("FLSocket", "Accepting connection on listenerFd %u",
	       listenerFd);
  
  int newfd = ::accept(listenerFd, &fromAddr, &fromSize);
  if (newfd < 0)
    {
      FLString msg("doAccept_() : accept failed: ");
      msg += strerror(errno);
      FLLogError("FLSocket", msg);
      throw FLException(msg);
    }

  // TODO: fix this. Something reasonable for now:
  int bufsize = 64 * 1024;
  int rval = ::setsockopt(newfd, SOL_SOCKET, SO_RCVBUF, (char *) &bufsize,
			  sizeof (bufsize));
  if (rval != 0)
    {
      FLString msg("doAccept_() : setsockopt failed: ");
      msg += strerror(errno);
      FLLogError("FLSocket", msg);
      throw FLException(msg);
    }
  
  P<FLSocket> newSocket(NEW, new FLSocket(*this));
  newSocket->fd_ = newfd;
  newSocket->cbClient_ = NULL;
  
  newSocket->initReadableCallback_(&FLSocket::readable_);

  // give the newly created socket to the user
  P<FLNotification> note(NEW, new
			 FLNotification(FLSubject::SSNull, this, newSocket));

  // should I really be storing the callback for a new connection in cbReadable_?
  cbClient_->invoke(note);
}


// "connect" to a port (with no host specified) only makes sense for broadcast
// packets.  The connect sets things up so that the caller will receive
// callbacks when there is data on the socket.

bool FLSocket::connect(FLCallBackBase &cb)
{
  // save his callback so I can tell him when I get notified that the socket is
  // readable. 
  cbClient_ = cb.clone();
  
  if( init_() )
    {
      // set things up so *I* am notfied that I am readable, then forward to my
      // user. 
      initReadableCallback_(&FLSocket::readable_);
      return true;
    }
  else
    return false;
}
 
bool FLSocket::connect(char *host, FLCallBackBase &cb)
{
  if (family_ != AF_INET)
    throw FLException("FLSocket::connect() family not implemented");

  init_();
  
  struct hostent* hp = gethostbyname(host);

  if( hp == 0 )
    {
      FLLogError("FLSocket", "connect() failed, Unknown hostname: %s\n", host);
      return false;
    }

  memcpy((char *)&socketStruct_.sin_addr, (char* )hp->h_addr, hp->h_length);
  socketStruct_.sin_port = htons(port_);
  int length = sizeof(socketStruct_);
  
  if( (::connect(fd_, (struct sockaddr *)&socketStruct_, length ) < 0 ) &&
      (errno != EINPROGRESS ) )
    {
      FLLogError("FLSocket", "Connecting SocketClient socket in FLSocket::connect: %s",
		 strerror(errno));
      return false;
    }

  notifyReadable(cb);
  initReadableCallback_(&FLSocket::readable_);
  return true;
}

int FLSocket::write(const FLBuffer &buffer,
		    sockaddr_in* outAddress)
{
  return write(buffer.contents(), buffer.size(), outAddress);
}

int
FLSocket::write(const void* buffer, int length, sockaddr_in* outAddress)
{
  FLAssertAlways(fd_ >= 0);

  // TODO: more error checking!

  int bytesSent = 0;
  
  if( type_ == SOCK_DGRAM )
    {
      if( !initialized_ )
	init_();
      if( outAddress )
	socketStruct_.sin_addr = outAddress->sin_addr;
      else
	socketStruct_.sin_addr.s_addr = INADDR_ANY;
      
      bytesSent = ::sendto(fd_, (const char*)buffer,
			   length, 0,
			   (const struct sockaddr *)&socketStruct_,
			   sizeof(socketStruct_));
    }
  else if( type_ == SOCK_STREAM )
    {
      if( initialized_ )
	bytesSent = ::send(fd_, (const char *)buffer, length, 0);
      else
	throw FLException("write on unconnected stream socket");
    }
  else 
    throw FLException("FLSocket type not implemented");
  
  if( bytesSent == -1 )
    throw FLException(FLPRINTF, "FLSocket::write() failed: %s",
		      strerror(errno));
  else if( bytesSent != length )
    {
      if( length/bytesSent < 10 )	// we are making progress :)
	return (bytesSent + write((char*)buffer+bytesSent,
				  length - bytesSent,
				  outAddress));
      else
	throw FLException(FLPRINTF,
			  "FLSocket::write failed: requested %d, wrote %d",
			  length, bytesSent);
    }
  return bytesSent;
}


NewP<FLBuffer> FLSocket::read(int n, sockaddr_in* address)
{
  int toRead;
  if (n == -1) {
    toRead = 4095;
  } else {
    toRead = n;
  }

  // Allocating one byte more than requested length to give us room
  // to add a terminating NULL.
  
  P<FLBuffer> buf(NEW, new FLBuffer(toRead+1));
  
  if( type_ == SOCK_STREAM )
    {
      // read toRead bytes, unless we specified n = -1, in which case
      // we just do a read and return
      int gotTotal = 0;
      do 
	{
	  int got = ::recv(fd_, gotTotal+(char*)buf->address(), 
			   toRead - gotTotal, 0);
	  if( got > 0 ) 
	    {
	      gotTotal += got;	// read successful: increment bytes read count
	    }
	  else if( got == 0 )
	    throw FLException("Socket has been closed");
	  else if (errno != EINTR)  // throw if we care about this failure
	    {
	      throw FLException("FLSocket::read(): recv error:",
				strerror(errno));
	    }
	}
      while ((gotTotal < toRead) && (n > 0));
      buf->setSize(gotTotal);
    }
  else if( type_ == SOCK_DGRAM )
    {
      struct sockaddr dst;
      socklen_t sockaddrSize = sizeof(dst);
      ssize_t size = 
	recvfrom(fd_, (char *)buf->contents(), buf->size(), 0,
		 &dst, &sockaddrSize);
      if( size < 0 )
	FLLogError("FLSocket", "read() error (recvfrom): %s", strerror(errno));
      if( address )
	*address = *(sockaddr_in*)&dst;
    }

  return NewP<FLBuffer>(buf);
}

NewP<FLBuffer> 
FLSocket::recv( size_t len, int flags )
{
  if( len < 0 ) {
    throw FLException( "FLSocket::recv(): called with negative length!" );
  }

  P<FLBuffer> bytes( NEW, new FLBuffer( len ) );
  
  int gotBytes = ::recv(fd_,
			static_cast<char*>( bytes->address() ), 
			len, flags );

  if( gotBytes < 0 ) {
    string errorString( "FLSocket::recv(): ::recv returned an error: " );
    errorString += strerror( errno );
    throw FLException( errorString );
  }

  // My thought is that we should expose as similar an interface as possible to
  // ::recv. 
  // Returning an FLBuffer instead of taking in a void* and returning a len is
  // a good enhancement though, IMO. (bhanoo: 2002/10/08 )

  bytes->setSize( gotBytes );
  return NewP<FLBuffer>( bytes );
}


bool FLSocket::setOutBufferSize(const int newSize)
{
  const int errCode = ::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, 
				   reinterpret_cast<const char *>(&newSize),
				   sizeof(newSize));
  if (errCode == -1) {
    FLLogWarning("FLSocket", "setOutBufferSize: error in ::setsockopt: %s",
		 strerror(errno));
  }
  return (errCode == 0) ? true : false;
}

int FLSocket::getOutBufferSize(void) const
{
  int result;
  socklen_t resultLen = sizeof(result);
  const int errCode = ::getsockopt(fd_, SOL_SOCKET, SO_SNDBUF,
				   reinterpret_cast<char *>(&result),
				   &resultLen);
  if (errCode == -1) {
    FLLogWarning("FLSocket", "getOutBufferSize: error in ::getsockopt: %s",
		 strerror(errno));
  }
  return (errCode == -1) ? -1 : result;
}



unsigned 
FLSocket::readable() const
{
  if( fd_ >= 0 )
    return FLPosixDevice(fd_).readable();
  else
    return 0;
}


void 
FLSocket::close()
{
  if( socketIOEventSpecifier_ )
    {
      socketIOEventSpecifier_->stop();
      socketIOEventSpecifier_ = 0;
    }
  if( fd_ >= 0 )
    {
      ::close(fd_);
      FLLogVerbose("FLSocket", "closing fd %u", fd_);
      fd_ = -1;
    }
}




/// Local Variables:
/// fill-column:79
/// End:

