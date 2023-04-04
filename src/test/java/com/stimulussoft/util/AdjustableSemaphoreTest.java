package com.stimulussoft.util;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Valentin Popov valentin@archiva.ru on 04.04.2023.
 */
public class AdjustableSemaphoreTest {

    @Test
    public void setMaxPermits() {
        AdjustableSemaphore semaphore = new AdjustableSemaphore();
        Assert.assertEquals(0, semaphore.availablePermits());

        for (int i = 1; i < 10; i++) {
            semaphore.setMaxPermits(i);
            Assert.assertEquals(i, semaphore.availablePermits());
        }

        for (int i = 10; i > 0; i--) {
            semaphore.setMaxPermits(i);
            Assert.assertEquals(i, semaphore.availablePermits());
        }

    }
}