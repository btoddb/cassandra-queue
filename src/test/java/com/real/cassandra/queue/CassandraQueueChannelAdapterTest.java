package com.real.cassandra.queue;

import static org.junit.Assert.assertEquals;

import java.util.Queue;

import javax.annotation.Resource;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.Message;
import org.springframework.integration.core.MessageBuilder;
import org.springframework.integration.core.MessageChannel;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.wyki.cassandra.pelops.Pelops;

import com.real.cassandra.queue.repository.QueueRepository;
import com.real.cassandra.queue.spring.CassandraQueueChannelAdapter;
import com.real.cassandra.queue.spring.MsgReceivedConsumer;

/**
 * Test the spring channel adapter, {@link CassandraQueueChannelAdapter}.
 * 
 * @author Todd Burruss
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:spring-cassandra-channels.xml", "classpath:spring-cassandra-queues.xml",
        "classpath:spring-config-properties.xml"

})
public class CassandraQueueChannelAdapterTest {
    @Autowired
    private QueueRepository qRep;

    @Resource(name = "testQueue")
    private CassQueue cq;

    @Autowired
    private MsgReceivedConsumer msgReceivedConsumer;

    @Resource(name = "testChannel")
    private MessageChannel testChannel;

    private TestUtils testUtils;

    @Test
    public void testPush() throws Exception {
        int numEvents = 100;
        for (int i = 0; i < numEvents; i++) {
            final Message<String> eventMsg = MessageBuilder.withPayload("xxx_" + i).build();
            testChannel.send(eventMsg);
        }

        verifyAllPopped(numEvents);
    }

    /**
     * Test channel automatically pop'ing from queue and delivering event via
     * channel direct to consumer object.
     * 
     * @throws Exception
     */
    @Test
    public void testPop() throws Exception {
        int numEvents = 100;
        for (int i = 0; i < numEvents; i++) {
            cq.push("xxx_" + i);
        }

        verifyAllPopped(numEvents);
    }

    // -----------------------

    private void verifyAllPopped(int numEvents) throws Exception {
        int lastNum = -1;
        for (;;) {
            int curNum = msgReceivedConsumer.getMsgQueue().size();
            if (curNum == lastNum) {
                break;
            }
            else if (0 <= curNum) {
                lastNum = curNum;
            }
            Thread.sleep(200);
        }

        Queue<String> msgQ = msgReceivedConsumer.getMsgQueue();
        System.out.println("msgQ = " + testUtils.outputStringsAsCommaDelim(msgQ));
        assertEquals("Events didn't get on channel properly: " + testUtils.outputStringsAsCommaDelim(msgQ), numEvents,
                msgQ.size());

        testUtils.verifyWaitingQueue(0);
        testUtils.verifyDeliveredQueue(0);
    }

    @Before
    public void setupTest() throws Exception {
        qRep.initCassandra(true);
        testUtils = new TestUtils(cq);
        msgReceivedConsumer.clear();
    }

    @AfterClass
    public static void shutdownQueueMgrAndPool() {
        Pelops.shutdown();
    }

}
