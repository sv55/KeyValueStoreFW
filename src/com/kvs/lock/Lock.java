// $Id$
package com.kvs.lock;

/**
* A lock interface.
*/

public interface Lock {

    public boolean lock();
    
    public boolean unlock();

}
