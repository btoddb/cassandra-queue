package com.real.cassandra.queue;

import static org.junit.Assert.*;

import org.junit.Test;

import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.pipes.PipeStatusFactory;

public class PipeStatusFactoryTest {

    @Test
    public void createValidFromRaw() {
        long now = System.currentTimeMillis();
        PipeStatusFactory psf = new PipeStatusFactory();
        PipeStatus psExpected = new PipeStatus("F", 123, now);

        assertEquals(psExpected, psf.createInstance("F/123/" + now));
        assertEquals(psExpected, psf.createInstance("    F/123  /" + now));
        assertEquals(psExpected, psf.createInstance("F    /     123/  " + now));
        assertEquals(psExpected, psf.createInstance("F/123/" + now + "   "));
        assertEquals(psExpected, psf.createInstance("     F/123/" + now));
    }

    @Test
    public void createRawFromValid() {
        long now = System.currentTimeMillis();
        PipeStatusFactory psf = new PipeStatusFactory();
        String psExpected = "F/123/" + now;

        assertEquals(psExpected, psf.createInstance(new PipeStatus("F", 123, now)));
    }
}
