/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.echobox.cache;

import static junit.framework.TestCase.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests for ExponentialBackOffRefillStrategy
 * @author eddspencer
 */
public class ExponentialBackOffRefillStrategyTest {

  private TickerMock tickerMock;
  private boolean backOff;

  @Before
  public void setUp() {
    tickerMock = new TickerMock();
    backOff = false;
  }

  /**
   * Test that the refill amount can account for multiple periods
   */
  @Test
  public void testBucketAddsMultiplePeriods() {
    final ExponentialBackOffRefillStrategy refillStrategy = createStrategy(1);
    assertEquals(10, refillStrategy.refill());
    tickerMock.setTime(1);
    assertEquals(10, refillStrategy.refill());
    tickerMock.setTime(3);
    assertEquals("Two periods have passed", 20, refillStrategy.refill());
  }

  /**
   * Test that the period will back off if prompted
   */
  @Test
  public void testBucketBacksOffExponentially() {
    final ExponentialBackOffRefillStrategy refillStrategy = createStrategy(1);
    backOff = true;
    assertEquals(10, refillStrategy.refill());
    tickerMock.setTime(2);
    assertEquals("One backed off period has passed", 10, refillStrategy.refill());
    tickerMock.setTime(6);
    backOff = false;
    assertEquals("Backed off again", 10, refillStrategy.refill());
    tickerMock.setTime(7);
    assertEquals("One normal period has passed", 10, refillStrategy.refill());
  }

  /**
   * Tests that the duration until next refill is calculated correctly
   */
  @Test
  public void testGetDurationUntilNextRefill() {
    final ExponentialBackOffRefillStrategy refillStrategy = createStrategy(10);
    assertEquals(10, refillStrategy.refill());
    tickerMock.setTime(6);
    assertEquals(4, refillStrategy.getDurationUntilNextRefill(TimeUnit.NANOSECONDS));
  }

  private ExponentialBackOffRefillStrategy createStrategy(final long period) {
    return new ExponentialBackOffRefillStrategy(tickerMock.getMockInstance(), 10, 2, period,
        TimeUnit.NANOSECONDS,
        () -> backOff);
  }
}
