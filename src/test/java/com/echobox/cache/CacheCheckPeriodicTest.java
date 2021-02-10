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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.gson.reflect.TypeToken;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Test that the CacheCheckPeriodic does only periodically check the cache
 *
 * @author eddspencer
 */
public class CacheCheckPeriodicTest {
  
  private static Logger logger = LoggerFactory.getLogger(CacheCheckPeriodicTest.class);
  
  private static final String CACHE_KEY = "test";
  private static final int CACHE_CHECK_PERIOD_SECONDS = 10;

  private static final TypeToken<Boolean> TYPE_TOKEN = new TypeToken<Boolean>() {};

  @Injectable
  private CacheService cacheService;
  
  @Injectable
  private Supplier<Long> unixTimeSupplier;

  private CacheCheckPeriodic<Boolean> cacheCheck;

  @Before
  public void setupMocks() {
    cacheCheck =
        new CacheCheckPeriodic<>(cacheService, "test", TYPE_TOKEN, CACHE_CHECK_PERIOD_SECONDS);
  }

  /**
   * Check that it reads correctly if cache is empty
   */
  @Test
  public void isEmptyIfNotInCache() {
    new Expectations() {
      {
        cacheService.tryGetCachedItem(CACHE_KEY, TYPE_TOKEN);
        result = null;
  
        cacheService.getUnixTimeSupplier();
        result = unixTimeSupplier;
        
        unixTimeSupplier.get();
        result = 1605893758;
      }
    };
    assertFalse(cacheCheck.get().isPresent());
  }

  /**
   * Check that it reads correctly if cache is set
   */
  @Test
  public void callCacheOnFirstGetOnly() {
    new Expectations() {
      {
        cacheService.tryGetCachedItem(CACHE_KEY, TYPE_TOKEN);
        result = true;
  
        cacheService.getUnixTimeSupplier();
        result = unixTimeSupplier;
  
        unixTimeSupplier.get();
        result = 1605893758;
      }
    };
    assertTrue(cacheCheck.get().orElse(false));
    assertTrue(cacheCheck.get().orElse(false));
  }

  /**
   * Check it changes value correctly after time period
   */
  @Test
  public void callsCacheAgainAfterPeriod() {
    new Expectations() {
      {
        cacheService.tryGetCachedItem(CACHE_KEY, TYPE_TOKEN);
        times = 2;
        returns(true, false);
  
        cacheService.getUnixTimeSupplier();
        result = unixTimeSupplier;
  
        unixTimeSupplier.get();
        result = 1605893758;
      }
    };

    assertTrue(cacheCheck.get().orElse(false));
    assertTrue(cacheCheck.get().orElse(false));
  
    new Expectations() {
      {
        unixTimeSupplier.get();
        result = 1605893758 + CACHE_CHECK_PERIOD_SECONDS + 1;
      }
    };
    
    // Set the time to be past the period so it will check cache again
    assertFalse("Read from cache second time", cacheCheck.get().orElse(true));
  }

  /**
   * Check it changes value even when continuously called during time period
   */
  @Test
  public void callsCacheContinuouslyButFlagChanged() {

    long currentUnixTime = 1605893758;
    
    new Expectations() {
      {
        cacheService.tryGetCachedItem(CACHE_KEY, TYPE_TOKEN);
        times = 2;
        returns(true, false);
  
        cacheService.getUnixTimeSupplier();
        result = unixTimeSupplier;
        
        unixTimeSupplier.get();
        returns(
            //Double unix time when initialising the cache
            currentUnixTime,
            currentUnixTime,
            currentUnixTime + 1,
            currentUnixTime + 2,
            currentUnixTime + 3,
            currentUnixTime + 4,
            currentUnixTime + 5,
            currentUnixTime + 6,
            currentUnixTime + 7,
            currentUnixTime + 8,
            currentUnixTime + 9,
            currentUnixTime + 10,
            currentUnixTime + 11
        );
      }
    };

    // Increase time in small steps, expect cache to be called after period since last value was
    // read from the cache itself. To fix bug where last timestamp was updated on every call not
    // every cache read.
    for (int i = 0; i <= CACHE_CHECK_PERIOD_SECONDS; i++) {
      assertTrue(cacheCheck.get().orElse(false));
    }

    assertFalse("Read from cache after period", cacheCheck.get().orElse(true));
  }
}
