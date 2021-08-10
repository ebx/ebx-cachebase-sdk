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
import static org.mockito.Mockito.when;

import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for CacheWithSupplierFailover
 * @author Daniyar
 */
public class CacheWithSupplierFailoverTest {
  
  private CacheService cacheService;
  
  @Before
  public void setUp() {
    cacheService = mock(CacheService.class);
  }
  
  @Test
  public void getFromSourceOfTruthTest() {
    when(cacheService.tryGetCachedItem(anyString(), any())).thenReturn(null);
    when(cacheService.isCacheAvailable()).thenReturn(true);
    when(cacheService.trySaveItemToCache(anyString(), eq(10), eq(10))).thenReturn(true);
    
    CacheWithSupplierFailover<Long> cache = new CacheWithSupplierFailover(cacheService,
        TypeToken.get(Long.TYPE), 10, 100);
    long valueFromSourceOfTruth = cache.getWithFailover("Test", () -> 16L);
    Assert.assertNotEquals(1L, valueFromSourceOfTruth);
    Assert.assertEquals(16L, valueFromSourceOfTruth);
  }
  
  @Test
  public void getFromCacheTest() {
    when(cacheService.tryGetCachedItem(anyString(), any())).thenReturn(1L); // value from cache
    when(cacheService.isCacheAvailable()).thenReturn(true);
    when(cacheService.trySaveItemToCache(anyString(), eq(10), eq(10))).thenReturn(true);
  
    CacheWithSupplierFailover<Long> cache = new CacheWithSupplierFailover(cacheService,
        TypeToken.get(Long.TYPE), 40, 100);
  
    long valueFromCache = cache.getWithFailover("Test", () -> 16L);
    
    Assert.assertNotEquals(16L, valueFromCache);
    Assert.assertEquals(1L, valueFromCache);
  }
  
  @Test
  public void getFromSourceWithFailover() {

    when(cacheService.tryGetCachedItem(eq("default-Test"), any())).thenReturn(null);
    when(cacheService.tryGetCachedItem(eq("error-Test"), any())).thenReturn(1L);
    when(cacheService.isCacheAvailable()).thenReturn(true);
    when(cacheService.trySaveItemToCache(anyString(), eq(10), eq(10))).thenReturn(true);
  
    CacheWithSupplierFailover<Long> cache = new CacheWithSupplierFailover(cacheService,
        TypeToken.get(Long.TYPE), 40, 100);
    
    long value = cache.getWithFailover("Test",
        () -> {
        throw new IllegalStateException("API call failed");
      });
    Assert.assertEquals(1L, value);
  }
  
  @Test
  public void getFromSourceWithFailoverMaxErrorIntervalReached() {
  
    when(cacheService.tryGetCachedItem(eq("default-Test"), any())).thenReturn(null);
    when(cacheService.tryGetCachedItem(eq("error-Test"), any())).thenReturn(null);
    when(cacheService.isCacheAvailable()).thenReturn(true);
    when(cacheService.trySaveItemToCache(anyString(), eq(10), eq(10))).thenReturn(true);
  
    CacheWithSupplierFailover<Long> cache = new CacheWithSupplierFailover(cacheService,
        TypeToken.get(Long.TYPE), 40, 100);

    IllegalStateException exception = Assert.assertThrows(IllegalStateException.class, () -> {
      cache.getWithFailover("Test",
          () -> {
          throw new IllegalStateException("API call failed");
        });
    });
    
    Assert.assertEquals("The maximum failover timeout of 100"
            + " seconds has expired.", exception.getMessage());
  }
}
