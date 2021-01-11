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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.reflect.TypeToken;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

/**
 * Tests for {@link WriteThroughCacheAsBytes}
 *
 * @author eddspencer
 */
public class WriteThroughCacheAsBytesTest {
  
  private static final int CACHE_EXPIRY_SECS = 100;
  private static final String KEY = "key-1";
  private static final Integer EMPTY_VALUE = -42;
  
  @Mocked
  private CacheService cacheService;
  
  private WriteThroughCache<Integer> cache;
  
  @Before
  public void setup() {
    cache = new WriteThroughCacheAsBytes<>(CACHE_EXPIRY_SECS, cacheService, EMPTY_VALUE);
  }
  
  @Test
  public void getAndCacheData() {
    final byte[] bytes = SerializationUtils.serialize(42);
    
    new Expectations() {
      {
        cacheService.isCacheAvailable();
        result = true;
        
        cacheService.toBytes(42);
        result = bytes;
        
        cacheService.trySaveItemToCache(KEY, CACHE_EXPIRY_SECS, bytes);
      }
    };
    
    final Optional<Integer> opData = cache.getAndCacheData(KEY, () -> Optional.of(42));
    assertTrue(opData.isPresent());
    assertEquals(42, opData.get().intValue());
  }
  
  @Test
  public void usesCachedData() {
    new Expectations() {
      {
        cacheService.tryGetCachedItemFromBytes(KEY, (TypeToken<Integer>) any);
        result = 43;
      }
    };
    
    final Optional<Integer> opData = cache.getAndCacheData(KEY, Optional::empty);
    assertTrue(opData.isPresent());
    assertEquals(43, opData.get().intValue());
  }
  
  @Test
  public void persistAndUpdateCache() {
    final byte[] bytes = SerializationUtils.serialize(42);
    new Expectations() {
      {
        cacheService.isCacheAvailable();
        result = true;
        
        cacheService.toBytes(42);
        result = bytes;
        
        cacheService.trySaveItemToCache(KEY, CACHE_EXPIRY_SECS, bytes);
      }
    };
    cache.persistAndUpdateCache(KEY, () -> 42);
  }
}
