package com.btoddb.cassandra.queue.utils;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

public class JmxMBeanManager {
    private static Object mbsCreateMonitor = new Object();
    private static JmxMBeanManager thisObj;

    private MBeanServer mbs;

    public JmxMBeanManager() {
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    public static final JmxMBeanManager getInstance() {
        synchronized (mbsCreateMonitor) {
            if (null == thisObj) {
                thisObj = new JmxMBeanManager();
            }
        }

        return thisObj;
    }

    public void registerMBean(Object theBean, String name) throws MalformedObjectNameException, NullPointerException,
            InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
        // try {
        ObjectName objName = new ObjectName(name);
        mbs.registerMBean(theBean, objName);
        // }
        // catch (Exception e) {
        // throw new RuntimeException("exception while registering MBean, " +
        // name, e);
        // }
    }

    public Object getAttribute(String objName, String attrName) {
        try {
            ObjectName on = new ObjectName(objName);
            Object obj = mbs.getAttribute(on, attrName);
            return obj;
        }
        catch (Exception e) {
            throw new RuntimeException("exception while getting MBean attribute, " + objName + ", " + attrName, e);
        }
    }

    public Integer getIntAttribute(String objName, String attrName) {
        return (Integer) getAttribute(objName, attrName);
    }

    public String getStringAttribute(String objName, String attrName) {
        return (String) getAttribute(objName, attrName);
    }
}
