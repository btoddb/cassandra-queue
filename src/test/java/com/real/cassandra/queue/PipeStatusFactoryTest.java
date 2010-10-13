package com.real.cassandra.queue;

import static org.junit.Assert.*;

import org.junit.Test;

import com.real.cassandra.queue.pipes.PipeProperties;
import com.real.cassandra.queue.pipes.PipePropertiesFactory;
import com.real.cassandra.queue.pipes.PipeStatus;

public class PipeStatusFactoryTest {

    @Test
    public void createValidFromRaw() {
        long now = System.currentTimeMillis();
        PipePropertiesFactory psf = new PipePropertiesFactory();
        PipeProperties psExpected = new PipeProperties(PipeStatus.NOT_ACTIVE, PipeStatus.ACTIVE, 123, now);

        assertEquals(psExpected, psf.createInstance("N/A/123/" + now));
        assertEquals(psExpected, psf.createInstance("    N/A/123  /" + now));
        assertEquals(psExpected, psf.createInstance("N / A    /     123/  " + now));
        assertEquals(psExpected, psf.createInstance("N/A/123/" + now + "   "));
        assertEquals(psExpected, psf.createInstance("     N/A/123/" + now));
    }

    @Test
    public void createRawFromValid() {
        long now = System.currentTimeMillis();
        PipePropertiesFactory psf = new PipePropertiesFactory();
        String psExpected = "C/C/123/" + now;

        assertEquals(psExpected,
                psf.createInstance(new PipeProperties(PipeStatus.COMPLETED, PipeStatus.COMPLETED, 123, now)));
    }
}
