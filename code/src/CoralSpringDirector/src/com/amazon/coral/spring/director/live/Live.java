package com.amazon.coral.spring.director.live;

/**
 * A simple interface to handle, in this order, "acquire" and "activate" (at startup), and "deactivate" and "release"
 * (at shutdown).
 * <p>
 * In general, it is expected that acquire and release may be lengthy operations. For example, an "acquire" could deal
 * with populating a memory cache from a database. On the other hand, "release" could countinue to serve existing
 * clients for a "graceful shutdown" period.
 * </p>
 * <p>
 * On the other hand, "activate" and "deactivate" should be quick. For example, "activate" may consist in listening to
 * port 80, while "deactivate" may consist in stopping listening for new clients.
 * </p>
 * <p>
 * While this interface was designed with these intentions in mind, the developer is free to use it as desired.
 * </p>
 * 
 * @author Ovidiu Gheorghies
 */
public interface Live {
    void acquire() throws Exception;

    void activate() throws Exception;

    void deactivate() throws Exception;

    void release() throws Exception;
}
