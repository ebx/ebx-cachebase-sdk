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
import com.echobox.cache.CachedResult;

import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * A non operational CacheService which can be used for testing
 * @author Michael Lavelle
 */
public class NoOpCacheService extends CacheService {

  /**
   * Default constructor
   * @param unixTimeSupplier Supplier to provide current unix times
   */
  public NoOpCacheService(final Supplier<Long> unixTimeSupplier) {
    super(TimeoutException.class, unixTimeSupplier);
  }
  
  /**
   * Default constructor
   */
  public NoOpCacheService() {
    super(TimeoutException.class, () -> (long) (System.currentTimeMillis() / 1000L));
  }

  @Override
  public boolean isCacheAvailable() {
    return true;
  }

  @Override
  public boolean tryAddItemToCache(String key, int cacheExpiryTimeSeconds, Object item) {
    return true;
  }

  @Override
  public long incr(String key, int by, int def, int cacheExpiryTimeSeconds) {
    return def;
  }

  @Override
  public boolean tryToIncrementOrDecrementCounterInCacheIfBelowLimit(String key,
      int cacheExpiryTimeSeconds, long limit, boolean decrement) {
    return true;
  }

  @Override
  public boolean trySaveItemToCache(String key, int cacheExpiryTimeSeconds, Object item) {
    return true;
  }

  @Override
  public boolean tryDeleteItemFromCache(String key) {
    return true;
  }
  
  @Override
  protected Optional<CachedResult<Object>> getRawCachedItemWithTtl(String key) throws Exception {
    return Optional.empty();
  }

  @Override
  protected Object getRawCachedItem(String key) throws Exception {
    return null;
  }
  
  @Override
  public int getNumActiveConnections() {
    return 0;
  }
}
