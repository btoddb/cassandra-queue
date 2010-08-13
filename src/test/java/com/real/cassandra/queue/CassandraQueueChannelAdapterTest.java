package com.real.cassandra.queue;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Queue;

import javax.annotation.Resource;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.wyki.cassandra.pelops.Pelops;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:spring-cassandra-channels.xml", "classpath:spring-cassandra-queues.xml",
        "classpath:spring-config-properties.xml"

})
public class CassandraQueueChannelAdapterTest {
    @Autowired
    private QueueRepository qRep;

    @Autowired
    private CassandraQueueChannelAdapter chAdap;

    @Resource(name = "testQueue")
    private CassQueue cq;

    @Autowired
    private MsgReceivedConsumer msgReceivedConsumer;

    private TestUtils testUtils;

    @Test
    public void testPushAndPop() throws Exception {
        testUtils.verifyWaitingQueue(0);
        testUtils.verifyDeliveredQueue(0);

        int numEvents = 100;
        for (int i = 0; i < numEvents; i++) {
            chAdap.push("xxx_" + i);
        }

        Thread.sleep(100); // must wait longer than the interval-trigger in
                           // inbound-channel-adapter
        int lastNum = 0;
        int curNum;
        while (lastNum != (curNum = msgReceivedConsumer.getMsgQueue().size())) {
            lastNum = curNum;
            Thread.sleep(100);
        }

        Queue<Event> msgQ = msgReceivedConsumer.getMsgQueue();
        System.out.println("msgQ = " + testUtils.outputEventsAsCommaDelim(msgQ));
        assertEquals("Events didn't get on channel properly: " + testUtils.outputEventsAsCommaDelim(msgQ), numEvents,
                msgQ.size());

        testUtils.verifyWaitingQueue(0);
        testUtils.verifyDeliveredQueue(0);
    }

    // -----------------------

    @Before
    public void setupQueueMgrAndPool() throws Exception {
        qRep.initCassandra(true);
        testUtils = new TestUtils(cq);
    }

    @AfterClass
    public static void shutdownQueueMgrAndPool() {
        Pelops.shutdown();
    }

}
