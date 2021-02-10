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

import com.google.gson.reflect.TypeToken;

import java.util.Optional;

/**
 * Only check the cache periodically for a given value, otherwise use the last value seen in
 * the cache. This abstracts the logic of 'should I call the cache' from the code so it can just
 * check the value
 *
 * @author eddspencer
 * @param <T> return type of cache object
 */
public class CacheCheckPeriodic<T> {

  private final CacheService cacheService;
  private final String key;
  private final TypeToken<T> returnType;
  private final int period;

  // Volatile to ensure if this class is called from multiple threads they all see correct value
  private volatile long lastTimeStampReadFromCache;
  private volatile T value;

  /**
   * Constructor
   *
   * @param cacheService the cache service 
   * @param key cache key to check 
   * @param returnType return type 
   * @param period period in seconds to check cache
   */
  public CacheCheckPeriodic(final CacheService cacheService, final String key,
      final TypeToken<T> returnType, final int period) {
    this.cacheService = cacheService;
    this.key = key;
    this.returnType = returnType;
    this.period = period;
    this.lastTimeStampReadFromCache = 0L; // Initialise to 0 so always checks cache first time
  }

  /**
   * Get the value from the cache or use previous value if last checked within period seconds
   *
   * @return optional object from cache
   */
  public Optional<T> get() {
    // Only call the cache once every period, otherwise use previous value
    if (cacheService.getUnixTimeSupplier().get() - period > lastTimeStampReadFromCache) {
      value = cacheService.tryGetCachedItem(key, returnType);
      lastTimeStampReadFromCache = cacheService.getUnixTimeSupplier().get();
    }
    return Optional.ofNullable(value);
  }

}
