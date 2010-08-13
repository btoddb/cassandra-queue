package com.real.cassandra.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;

import org.apache.cassandra.thrift.Column;

public class TestUtils {
    private CassQueue cq;

    public TestUtils(CassQueue cq) {
        this.cq = cq;
    }

    public String outputEventsAsCommaDelim(Collection<Event> collection) {
        if (null == collection) {
            return null;
        }

        if (collection.isEmpty()) {
            return "";
        }

        StringBuilder sb = null;
        for (Event evt : collection) {
            if (null != sb) {
                sb.append(", ");
            }
            else {
                sb = new StringBuilder();
            }

            sb.append(evt.getValue());
        }

        return sb.toString();
    }

    public String outputColumnsAsCommaDelim(Collection<Column> collection) {
        if (null == collection) {
            return null;
        }

        if (collection.isEmpty()) {
            return "";
        }

        StringBuilder sb = null;
        for (Column col : collection) {
            if (null != sb) {
                sb.append(", ");
            }
            else {
                sb = new StringBuilder();
            }

            sb.append(new String(col.getValue()));
        }

        return sb.toString();
    }

    public void verifyExistsInDeliveredQueue(int index, int numEvents, boolean wantExists) throws Exception {
        List<Column> colList = cq.getDeliveredEvents(index % cq.getNumPipes(), numEvents + 1);
        if (wantExists) {
            boolean found = false;
            for (Column col : colList) {
                if (new String(col.getValue()).equals("xxx_" + index)) {
                    found = true;
                    break;
                }
            }
            assertTrue("should have found value, xxx_" + index + " in delivered queue", found);
        }
        else {
            for (Column col : colList) {
                assertNotSame(new String(col.getValue()), "xxx_" + index);
            }
        }

    }

    public void verifyExistsInWaitingQueue(int index, int numEvents, boolean wantExists) throws Exception {
        List<Column> colList = cq.getWaitingEvents(index % cq.getNumPipes(), numEvents + 1);
        if (wantExists) {
            boolean found = false;
            for (Column col : colList) {
                if (new String(col.getValue()).equals("xxx_" + index)) {
                    found = true;
                    break;
                }
            }
            assertTrue("should have found value, xxx_" + index + " in waiting queue", found);
        }
        else {
            for (Column col : colList) {
                assertNotSame(new String(col.getValue()), "xxx_" + index);
            }
        }

    }

    public void verifyDeliveredQueue(int numEvents) throws Exception {
        int min = numEvents / cq.getNumPipes();
        int mod = numEvents % cq.getNumPipes();

        for (int i = 0; i < cq.getNumPipes(); i++) {
            List<Column> colList = cq.getDeliveredEvents(i, numEvents + 1);
            assertEquals("count on queue index " + i + " is incorrect", i < mod ? min + 1 : min, colList.size());

            for (int j = 0; j < colList.size(); j++) {
                String value = new String(colList.get(j).getValue());
                assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
            }
        }
    }

    public void verifyWaitingQueue(int numEvents) throws Exception {
        int min = numEvents / cq.getNumPipes();
        int mod = numEvents % cq.getNumPipes();

        for (int i = 0; i < cq.getNumPipes(); i++) {
            List<Column> colList = cq.getWaitingEvents(i, numEvents + 1);
            assertEquals("count on queue index " + i + " is incorrect: events = " + outputColumnsAsCommaDelim(colList),
                    i < mod ? min + 1 : min, colList.size());

            for (int j = 0; j < colList.size(); j++) {
                String value = new String(colList.get(j).getValue());
                assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
            }
        }
    }

}
