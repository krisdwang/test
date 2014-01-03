/**
 * 
 * Conduct parallelized, ordered event dispatch to Spring beans upon context startup and shutdown.
 * <p>
 * Dispatching is done according to specific plans, which are defined as Spring beans. The key aspects are parallel
 * message dispatch and continuation upon successful completion.
 * </p>
 * <p>
 * For more information, see <a href="https://w.amazon.com/index.php/Coral/Internal/SpringDirector">wiki docs</a>.
 * </p>
 */
package com.amazon.coral.spring.director;

