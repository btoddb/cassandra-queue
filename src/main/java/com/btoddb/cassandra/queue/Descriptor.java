package com.btoddb.cassandra.queue;

import java.io.Serializable;

/**
 * To be implemented by classes that describe components and can provide them a unique ID. Useful for example in gaining
 * a lock to those described objects.
 *
 * @author Andrew Ebaugh
 * @version $Id: Descriptor.java,v 1.1 2010/10/29 20:33:23 aebaugh Exp $
 */
public interface Descriptor {

    Serializable getId();
}
