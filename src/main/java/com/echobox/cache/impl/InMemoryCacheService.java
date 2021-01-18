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

package com.echobox.cache.impl;

import com.echobox.cache.CacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * A basic cache service which offers core functionality using an in memory hashmap.
 * IMPORTANT NOTE: Currently data never expires from this cache.
 * @author MarcF
 */
public class InMemoryCacheService extends CacheService {
  
  private static Logger logger = LoggerFactory.getLogger(InMemoryCacheService.class);
  
  /**
   * The cache as a HashMap.
   */
  private Map<String, Object> cache = new ConcurrentHashMap<>();

  /**
   * Constructor.
   * @param unixTimeSupplier Supplier to provide current unix times
   */
  public InMemoryCacheService(final Supplier<Long> unixTimeSupplier) {
    super(TimeoutException.class, unixTimeSupplier);
  }
  
  /**
   * Constructor.
   */
  public InMemoryCacheService() {
    super(TimeoutException.class, () -> (long) (System.currentTimeMillis() / 1000L));
  }

  @Override
  public boolean trySaveItemToCache(String key, int cacheExpiryTimeSeconds, Object item) {
    
    cache.put(key, item);
    
    if (cacheExpiryTimeSeconds != 0) {
      logger.warn("Added item in cache with key " + key + " and a non zero cache expiry time. "
          + "Please note that data never expires from this implementation.");
    }
    
    return true;
  }

  @Override
  protected Object getRawCachedItem(String key) throws Exception {
    return cache.get(key);
  }
  
  @Override
  public boolean isCacheAvailable() {
    return true;
  }

  @Override
  public boolean tryAddItemToCache(String key, int cacheExpiryTimeSeconds, Object item) {
    //The add should only succeed if the cache does not contain the key
    if (!cache.containsKey(key)) {
      return trySaveItemToCache(key, cacheExpiryTimeSeconds, item);
    } else {
      return false;
    }
  }

  @Override
  public long incr(String key, int by, int def, int cacheExpiryTimeSeconds) {
    throw new UnsupportedOperationException("This functionality is not supported by "
        + "this implementation.");
  }

  @Override
  public boolean tryToIncrementOrDecrementCounterInCacheIfBelowLimit(String key,
      int cacheExpiryTimeSeconds, long limit, boolean decrement) {
    throw new UnsupportedOperationException("This functionality is not supported by "
        + "this implementation.");
  }

  @Override
  public boolean tryDeleteItemFromCache(String key) {
    cache.remove(key);
    return true;
  }
  
  @Override
  public int getNumActiveConnections() {
    return 0;
  }
}

