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

import com.google.common.base.Ticker;
import org.isomorphism.util.TokenBucket;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Token bucket that will refill at a constant rate until the bucket is empty and then 
 * exponentially back off the refill rate until the bucket has tokens in it again, at which point
 * it will continue to refill as normal at the original rate.
 *
 * @author eddspencer
 */
public class ExponentialBackOffRefillStrategy implements TokenBucket.RefillStrategy {

  private final Ticker ticker;
  private final long numTokensPerPeriod;
  private final long periodInitial;
  private final double factorBackOff;
  private final Supplier<Boolean> shouldBackOff;
  private long periodCurrent;
  private long lastRefillTime;
  private long nextRefillTime;

  /**
   * Instantiates a new Exponential back off refill strategy.
   *
   * @param ticker the ticker 
   * @param numTokensPerPeriod the num tokens per period 
   * @param factorBackOff the factor back off 
   * @param period the period 
   * @param unit the unit 
   * @param shouldBackOff the should back off
   */
  public ExponentialBackOffRefillStrategy(Ticker ticker, long numTokensPerPeriod,
      double factorBackOff, long period, TimeUnit unit, Supplier<Boolean> shouldBackOff) {
    this.ticker = ticker;
    this.numTokensPerPeriod = numTokensPerPeriod;
    this.periodInitial = unit.toNanos(period);
    this.periodCurrent = periodInitial;
    this.factorBackOff = factorBackOff;
    this.shouldBackOff = shouldBackOff;

    lastRefillTime = -periodInitial;
    nextRefillTime = -periodInitial;
  }

  @Override
  public long refill() {
    long now = ticker.read();
    if (now < nextRefillTime) {
      return 0;
    }

    // We now know that we need to refill the bucket with some tokens, the question is how many. 
    // We need to count how many periods worth of tokens we've missed.
    long numPeriods = Math.max(0, (now - lastRefillTime) / periodCurrent);

    // Move the last refill time forward by this many periods.
    lastRefillTime += numPeriods * periodCurrent;

    // Recalculate the current period
    if (Boolean.TRUE.equals(shouldBackOff.get())) {
      // Back off by given factor
      periodCurrent = (long) (periodCurrent * factorBackOff);
    } else {
      // Revert to initial period
      periodCurrent = periodInitial;
    }

    // We will refill again one period after the last time we refilled.
    nextRefillTime = lastRefillTime + periodCurrent;

    return numPeriods * numTokensPerPeriod;
  }

  @Override
  public long getDurationUntilNextRefill(final TimeUnit unit) {
    long now = ticker.read();
    return unit.convert(Math.max(0, nextRefillTime - now), TimeUnit.NANOSECONDS);
  }
}
