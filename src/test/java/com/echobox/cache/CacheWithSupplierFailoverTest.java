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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.echobox.time.UnixTime;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

/**
 * Test for CacheWithSupplierFailover
 * @author Daniyar
 */
public class CacheWithSupplierFailoverTest {
  
  private CacheService cacheService;
  private MockedStatic<UnixTime> unixTime;
  
  @Before
  public void setUp() {
    cacheService = mock(CacheService.class);
    unixTime = mockStatic(UnixTime.class);
  }
  
  @Test
  public void getFromSourceOfTruthTest() {
    unixTime.when(UnixTime::now).thenReturn(100L);
    when(cacheService.tryGetCachedItem(anyString(), any())).thenReturn(1L);
    when(cacheService.isCacheAvailable()).thenReturn(true);
    when(cacheService.trySaveItemToCache(anyString(), eq(10), eq(10))).thenReturn(true);
    
    CacheWithSupplierFailover<Long> cache = new CacheWithSupplierFailover(cacheService,
        TypeToken.get(Long.TYPE), 10, 10);
    long valueFromSourceOfTruth = cache.getWithFailover("Test", () -> 16L);
    Assert.assertNotEquals(1L, valueFromSourceOfTruth);
    Assert.assertEquals(16L, valueFromSourceOfTruth);
    unixTime.close();
  }
  
  @Test
  public void getFromCacheTest() {
    unixTime.when(UnixTime::now).thenReturn(150L);
    when(cacheService.tryGetCachedItem(anyString(), any())).thenReturn(1L); // value from cache
    when(cacheService.isCacheAvailable()).thenReturn(true);
    when(cacheService.trySaveItemToCache(anyString(), eq(10), eq(10))).thenReturn(true);
  
    CacheWithSupplierFailover<Long> cache = new CacheWithSupplierFailover(cacheService,
        TypeToken.get(Long.TYPE), 40, 10);
  
    long valueFromSourceOfTruth = cache.getWithFailover("Test", () -> 16L);
    // 10 seconds passed
    unixTime.when(UnixTime::now).thenReturn(160L);
    long valueFromCache = cache.getWithFailover("Test", () -> 16L);
    Assert.assertEquals(1L, valueFromCache);
    // another 10 seconds passed
    unixTime.when(UnixTime::now).thenReturn(170L);
    valueFromCache = cache.getWithFailover("Test", () -> 16L);
    Assert.assertEquals(1L, valueFromCache);
    // more 10 seconds passed, should still get value from cache
    unixTime.when(UnixTime::now).thenReturn(180L);
    valueFromCache = cache.getWithFailover("Test", () -> 16L);
    Assert.assertEquals(1L, valueFromCache);
    // 15 seconds passed, now should get value from source of truth
    unixTime.when(UnixTime::now).thenReturn(195L);
    long value = cache.getWithFailover("Test", () -> 16L);
    Assert.assertEquals(16L, value);
    unixTime.close();
  }
  
  @Test
  public void getFromSourceWithFailover() {
    unixTime.when(UnixTime::now).thenReturn(150L);
    when(cacheService.tryGetCachedItem(anyString(), any())).thenReturn(1L); // value from cache
    when(cacheService.isCacheAvailable()).thenReturn(true);
    when(cacheService.trySaveItemToCache(anyString(), eq(10), eq(10))).thenReturn(true);
  
    CacheWithSupplierFailover<Long> cache = new CacheWithSupplierFailover(cacheService,
        TypeToken.get(Long.TYPE), 40, 10);
  
    long valueFromSourceOfTruth = cache.getWithFailover("Test", () -> 16L);
    // 10 seconds passed
    unixTime.when(UnixTime::now).thenReturn(160L);
    long valueFromCache = cache.getWithFailover("Test", () -> 16L);
    Assert.assertEquals(1L, valueFromCache);
    
    // The source of truth has failed, so we should get cached value
    unixTime.when(UnixTime::now).thenReturn(260L);
    long value = cache.getWithFailover("Test",
        () -> {
        throw new IllegalStateException("API call failed");
      });
    Assert.assertEquals(1L, valueFromCache);
    unixTime.close();
  }
  
  @Test
  public void getFromSourceWithFailoverMaxErrorIntervalReached() {
    unixTime.when(UnixTime::now).thenReturn(150L);
    when(cacheService.tryGetCachedItem(anyString(), any())).thenReturn(1L); // value from cache
    when(cacheService.isCacheAvailable()).thenReturn(true);
    when(cacheService.trySaveItemToCache(anyString(), eq(10), eq(10))).thenReturn(true);
  
    CacheWithSupplierFailover<Long> cache = new CacheWithSupplierFailover(cacheService,
        TypeToken.get(Long.TYPE), 40, 10);
  
    long valueFromSourceOfTruth = cache.getWithFailover("Test", () -> 16L);
    // 10 seconds passed
    unixTime.when(UnixTime::now).thenReturn(160L);
    long valueFromCache = cache.getWithFailover("Test", () -> 16L);
    Assert.assertEquals(1L, valueFromCache);
  
    // The source of truth has failed, so we should get cached value
    unixTime.when(UnixTime::now).thenReturn(260L);
    long value = cache.getWithFailover("Test",
        () -> {
        throw new IllegalStateException("API call failed");
      });
    Assert.assertEquals(1L, valueFromCache);
    
    // The source of truth is failing for long time. Giving up
    unixTime.when(UnixTime::now).thenReturn(300L);
    IllegalStateException exception = Assert.assertThrows(IllegalStateException.class, () -> {
      cache.getWithFailover("Test",
          () -> {
          throw new IllegalStateException("API call failed");
        });
    });
    
    Assert.assertEquals("We could not get the value from the source of truth for 40 seconds."
            + " The maximum wait interval is 10 seconds. Giving up", exception.getMessage());
    unixTime.close();
  }
}
