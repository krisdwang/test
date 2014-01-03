#ifndef _FLSOCKET_H
#define _FLSOCKET_H

static const char* const _FLSocket_h_rcs = __FILE__

"$Id: //brazil/src/appgroup/webservices/apps/DThrottle/mainline/throttle/FLSocket2.h#1 $";

/**
   @file	FLSocket.h
   @author		Jay Sipelstein (sipelste@susq.com)
   @date		Wed Sep 23 11:07:16 1998

   Copyright (c) 1998 Susquehanna Partners, G.P. All rights reserved.

   @ingroup FOL
*/

/**
   @class FLSocket
   @ingroup FOL

   Put all interactions with sockets in one class.  
   
   Class has been designed to use notifications and callbacks to tell clients
   when there has been activity on a socket.  Class uses FLBuffer to avoid
   clients having to deal with memory allocation.
*/

#if !defined(_WIN32)
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#endif

#include "flcore/FLObject.h"

#warning Remove the FLSocket2 when FLSocket supports AF_UNIX
#define FLSocket FLSocket2

class FLBuffer;
class FLIoEventSpecifier;
class FLCallBackBase;
class FLNotification;

/*
  There is still a considerable amount of work to be done with this class.
  We're reasonably happy with it as a notification based approach to using
  sockets and as a way to hide the "metal" from users.  
*/

class FLSocket : public FLObject
{
  DECLARE_CLASS(FLSocket, FLObject);
  DECLARE_VISITABLE(FLSocket);		// prevent warnings about hiding accept
  
public:
  enum { SocketMaxClients = 32 };
  
  virtual 			~FLSocket();
  
  /**
     construct an AF_INET socket and bind it to a port */
  FLSocket( in_port_t	portNum,
	    int 	type = SOCK_STREAM,
	    int		family = AF_INET,
	    int		protocol = IPPROTO_IP);
  
  /**
     construct an AF_UNIX socket and bind it to a path */
  FLSocket( const char *path,
	    int 	type = SOCK_STREAM,
	    int		family = AF_UNIX,
	    int		protocol = 0);
  
  /**
     construct non-bound socket */
  FLSocket();
  
  FLSocket(FLClassFlag);
  
  // General Use methods:
  
  /** Accept connections over the socket, calling back with a notification
      containing a new socket for communication over the connected socket.  NB: It
      is the called backed function's responsibility to store a smart pointer to
      the socket (in the notification) somewhere, or it will be deleted upon
      return to accept.  
  */
  bool 			acceptConnection(FLCallBackBase &cb,
					 int maxClientBacklog = SocketMaxClients);
  
  /**
     Connect to a specified host, calling back with socket of connection.
     probably has to have some retry/timeout logic (don't need this yet) 
  */
  bool 			connect(char *host, FLCallBackBase &cb);
  
  /**
     Do broadcast-based connection: broadcast datagram, whoever is listening
     might try to connect. After constructing a DGRAM socket, use this connect
     function and you will start receiving notifications when there is data (in
     other words, this m.f. does a notify readable for you).  
  */
  bool 			connect(FLCallBackBase &cb);

  /**
     Read n bytes, -1 means read as much as possible. Need way to specify
     blocking or not. Use blocking for now. If address is specified and socket is
     DGRAM, fill in socket struct values from recvfrom.  Returns a buffer with
     the read data.  Throws FLException (string contains strerr) if read fails.  
  */
  NewP<FLBuffer> 	read(int n = -1, sockaddr_in* address = NULL);

  /**
     a wrapper for ::recv; read upto 'len' bytes into a buffer.
     
     The flags field is optional, and formed by OR'ing one or more of:
          MSG_OOB | MSG_PEEK | MSG_WAITALL | MSG_NOSIGNAL | MSG_ERRQUEUE

	  'man recv' for an explanation of these flags.

     @return: NewP<FLBuffer>, with its size set to the actual number of bytes
              that were read in.

     Throws an FLException iff:
     - Input 'length' < 0
     - return value of ::recv < 0 ( FLException contains strerror(errno) )
  */
  
  NewP<FLBuffer> 	recv( size_t len, int flags = 0 );

  /**
     Write contents of buffer to socket (appropriately for the type of
     socket). Return count of chars written. The outAddress variable is only
     used when doing UDP datagram writes, and becomes the target address;
     otherwise broadcast is done.  Throws FLException if write fails.  Since
     any case where all the bytes are NOT written is considered a failure, it
     really isn't THAT useful to return the number of bytes written ... but we
     do so anyway on the off chance that someone wants to do retries, and to be
     consistent with the ::write() interface.
     
  */
  int 			write(const FLBuffer &buffer,
			      sockaddr_in* outAddress = NULL);
  int 			write(const void* buffer, int length,
			      sockaddr_in* outAddress = NULL);

  /**
     Client uses this to tell me what to call back when I am readable.  
  */
  void			notifyReadable(const FLCallBackBase &cb);

  /** @return number of bytes currently readable from socket
   */
  unsigned		readable() const;
  
  void			close();
  
  // The next several methods allow you to modify or access parameters
  // associated with the socket.

  /**
     set/get the size (in bytes) of the output buffer for the socket. If you are
     using blocking writes, this determines how much data can be buffered up
     before the write blocks. If you are using non-blocking writes, this
     determines how much data can stored before the write fails (with EWOULDBLOCK
     as the errno). setOutBufferSize returns true on success, false on
     failure. getOutBufferSize returns -1 on failure.  
  */
  bool			setOutBufferSize(const int newSize);
  int			getOutBufferSize(void) const;

  /**
     Return the underlying stuff since otherwise this interface will be too big.  
  */
  int socket() { return fd_; }
  struct sockaddr *	addr();

  // superclass declarations:

  virtual FLString 		print() const;
  virtual bool 			isValid() const;

  // end superclass declarations
  
private:
  // member variables

  int			fd_;
  in_port_t		port_;
  std::string		path_;
  int			type_;
  int			family_;
  int			protocol_;

  sockaddr_in		socketStruct_;
  sockaddr_un		socketStructUnix_;
  
  bool			initialized_;

  P<FLIoEventSpecifier> socketIOEventSpecifier_;

  P<FLCallBackBase>	cbClient_;
    /// If my user has given me a callback to invoke when I become readable it
    /// is stored here

private:     

  // FLSocket(const FLSocket&);			// use default
  
  FLSocket& 	operator=(const FLSocket&);	// not implemented

  void 			readable_(const FLNotification * notification);
    /// my callback function for when I become readable

  void 			doAccept_(const FLNotification *notification);
    /// my callback function for a server socket receiving a connection

  bool			init_();
    /// do common c level initialization

  void 			doAcceptBroadcast_(const FLNotification * note);
    /// forward readable broadcast notification to user

  FLSocket(int fd, const sockaddr_in& socketStruct);
    /// this is used internally to construct the connected socket

  void	initReadableCallback_ (void (FLSocket::*cb)(const FLNotification*));
    /// this is used to set the callback from the OS to the correct internal
    /// function for handling the "readable" state (which in reality is
    /// overloaded to mean almost everything)


};	 			// class FLSocket

//////////////////////////////////////////////////////////////////////////
//                                                                      //
//                               Inlines                                //
//                                                                      //
//////////////////////////////////////////////////////////////////////////



/// Local Variables:
/// fill-column:79
/// End:
#endif		// _FLSOCKET_H

