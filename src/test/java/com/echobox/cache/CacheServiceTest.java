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
import static org.junit.Assert.fail;

import com.echobox.shutdown.impl.SimpleShutdownMonitor;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for CacheService
 * @author eddspencer
 */
public class CacheServiceTest {

  private static final TypeToken<String> TYPE_TOKEN = new TypeToken<String>() {};
  private static final String KEY = "key";

  private SimpleCacheService cacheService;
  private boolean origCacheInitialised;
  private TickerMock mockTicker;

  /**
   * Setup mocks and cache
   */
  @Before
  public void setup() {
    mockTicker = new TickerMock();
    cacheService = new SimpleCacheService(TimeoutException.class);
    origCacheInitialised = CacheService.initialised;
    CacheService.initialised = true;
    CacheService.timeoutBucket = null;
  }

  /**
   * Reset cache
   */
  @After
  public void tearDown() {
    CacheService.initialised = origCacheInitialised;
  }

  /**
   * Test to check that the cache available correctly if the token bucket is null
   */
  @Test
  public void testCacheServiceInitialisedWithoutTokenBucket() {
    assertTrue(cacheService.isCacheAvailable());
  }

  /**
   * Test to check that the cache is not available correctly if the token bucket is null
   */
  @Test
  public void testCacheServiceNotInitialisedWithoutTokenBucket() {
    CacheService.initialised = false;
    assertFalse(cacheService.isCacheAvailable());
  }

  /**
   * Test to check that the cache is available correctly if the token bucket has tokens
   */
  @Test
  public void testCacheServiceAvailableIfTokenBucketHasTokens() {
    CacheService.setTimeoutStrategy(2, 2.0);
    mockTicker.setTime(TimeUnit.SECONDS.toNanos(1));

    cacheService.mockCacheTimeout();
    assertTrue(cacheService.isCacheAvailable());
    
    mockTicker.setTime(TimeUnit.SECONDS.toNanos(2));
    cacheService.mockCacheTimeout();
    assertTrue(cacheService.isCacheAvailable());
  }

  /**
   * Test to check that the cache is disabled correctly if the token bucket is empty
   */
  @Test
  public void testCacheServiceDisabledByEmptyTokenBucket() {
    CacheService.setTimeoutStrategy(2, 1.0);
    cacheService.mockCacheTimeout();
    cacheService.mockCacheTimeout();
    assertFalse(cacheService.isCacheAvailable());
  }

  /**
   * Test the cache bucket is re-enabled after the backoff period
   */
  @Test
  public void testCacheReEnablesAfterMaxTimeouts() {
    CacheService.setTimeoutStrategy(2, 2.0);
    cacheService.mockCacheTimeout();
    cacheService.mockCacheTimeout();
    assertFalse("Max timeouts reached", cacheService.isCacheAvailable());

    mockTicker.setTime(TimeUnit.SECONDS.toNanos(1));
    assertFalse("After 1 second cache should still be disabled", cacheService.isCacheAvailable());

    mockTicker.setTime(TimeUnit.SECONDS.toNanos(2));
    assertTrue("After 2 seconds cache should be re-enabled", cacheService.isCacheAvailable());
  }

  /**
   * Test that a set of exceptions can be provided that are thrown directly
   */
  @Test
  public void testThrowsGivenExceptionDirectly() {
    cacheService.exceptionToThrow = new NumberFormatException("Testing");
    try {
      cacheService.tryGetCachedItem(KEY, TYPE_TOKEN,
          Sets.newHashSet(IllegalStateException.class, NumberFormatException.class));
      fail("Expect exception");
    } catch (NumberFormatException ex) {
      assertFalse(cacheService.handleExceptionCalled);
      assertFalse(cacheService.cacheTimeout);
    }
  }

  /**
   * Test that timeout exceptions are handled and not thrown
   */
  @Test
  public void testHandleTimeoutException() {
    cacheService.exceptionToThrow = new TimeoutException("Timeout");
    cacheService.elapsedTime = 20;
    cacheService.tryGetCachedItem(KEY, TYPE_TOKEN);

    assertTrue(cacheService.cacheTimeout);
  }

  /**
   * Test that timeout exceptions type can be set
   */
  @Test
  public void testCanSetTimeoutExceptionType() {
    cacheService = new SimpleCacheService(CheckedOperationTimeoutException.class);
    cacheService.exceptionToThrow =
        new CheckedOperationTimeoutException("Timeout", Collections.emptyList());
    cacheService.elapsedTime = 20;
    cacheService.tryGetCachedItem(KEY, TYPE_TOKEN);

    assertTrue(cacheService.cacheTimeout);
  }

  /**
   * Test validate null exceptions to throw
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidateNullExceptionsToThrow() {
    cacheService.tryGetCachedItem(KEY, TYPE_TOKEN, null);
  }

  /**
   * Simple cache service for use in tests
   *
   * @author eddspencer
   */
  private static class SimpleCacheService extends CacheService {

    private int elapsedTime;
    private Exception exceptionToThrow;
    private boolean handleExceptionCalled;
    private boolean cacheTimeout;

    static {
      shutdownMonitor = new SimpleShutdownMonitor();
    }

    private SimpleCacheService(final Class<? extends Exception> classTimeoutException) {
      super(classTimeoutException, () -> (long) (System.currentTimeMillis() / 1000L));
    }

    private void mockCacheTimeout() {
      onCacheTimeout();
    }

    @Override
    public long incr(final String key, final int by, final int def,
        final int cacheExpiryTimeSeconds) {
      return 0;
    }

    @Override
    public boolean tryToIncrementOrDecrementCounterInCacheIfBelowLimit(final String key,
        final int cacheExpiryTimeSeconds, final long limit, final boolean decrement) {
      return false;
    }

    @Override
    protected Object getRawCachedItem(final String key) throws Exception {
      if (exceptionToThrow != null) {
        throw exceptionToThrow;
      }
      return null;
    }

    @Override
    public boolean trySaveItemToCache(final String key, final int cacheExpiryTimeSeconds,
        final Object item) {
      return false;
    }

    @Override
    public boolean tryAddItemToCache(final String key, final int cacheExpiryTimeSeconds,
        final Object item) {
      return false;
    }

    @Override
    public boolean tryDeleteItemFromCache(final String key) {
      return false;
    }

    @Override
    protected void handleException(final String key, final long startTime,
        final Exception getException, final int expectedCacheTimeoutSecs, final String action) {
      handleExceptionCalled = true;
      super.handleException(key, startTime - elapsedTime, getException, expectedCacheTimeoutSecs,
          action);
    }

    @Override
    protected void onCacheTimeout() {
      cacheTimeout = true;
      super.onCacheTimeout();
    }
  
    @Override
    public int getNumActiveConnections() {
      return 0;
    }
  }
}
