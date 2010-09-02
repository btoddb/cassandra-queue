package com.real.cassandra.queue.pipeperpusher;

import static org.junit.Assert.*;

import org.junit.Test;

public class PipeStatusFactoryTest {

    @Test
    public void createValidFromRaw() {
        PipeStatusFactory psf = new PipeStatusFactory();
        PipeStatus psExpected = new PipeStatus("F", 123);

        assertEquals(psExpected, psf.createInstance("F/123"));
        assertEquals(psExpected, psf.createInstance("    F/123  "));
        assertEquals(psExpected, psf.createInstance("F    /     123"));
        assertEquals(psExpected, psf.createInstance("F/123    "));
        assertEquals(psExpected, psf.createInstance("     F/123"));
    }

    @Test
    public void createRawFromValid() {
        PipeStatusFactory psf = new PipeStatusFactory();
        String psExpected = "F/123";

        assertEquals(psExpected, psf.createInstance(new PipeStatus("F", 123)));
    }
}
