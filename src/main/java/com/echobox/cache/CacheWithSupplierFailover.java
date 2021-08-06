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
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Periodically executes the callback to getWithFailover the latest value.
 * If it's within the specified period then getWithFailover the cached value.
 * If the callback fails still getWithFailover the cached value and update the cache next time
 * @param <T> return type of cache object
 * @author Daniyar
 */
public class CacheWithSupplierFailover<T extends Serializable> {
  
  private final int defaultCacheSecs;
  
  private final int maxCacheSecsOnError;
  
  private final CacheService cacheService;
  
  private final TypeToken<T> returnType;
  
  /**
   * Constructor
   *
   * @param cacheService the cache service
   * @param returnType return type
   * @param defaultCacheSecs period in seconds during which we use cache
   * @param maxCacheSecsOnError The maximum interval we will continue to use the cached value
   * if the 'source of truth' supplier isn't working for whatever reason.
   */
  public CacheWithSupplierFailover(CacheService cacheService, TypeToken<T> returnType,
      int defaultCacheSecs, int maxCacheSecsOnError) {
    this.returnType = returnType;
    this.defaultCacheSecs = defaultCacheSecs;
    this.maxCacheSecsOnError = maxCacheSecsOnError;
    this.cacheService = cacheService;
  }
  
  /**
   * Check if it is within callback period. If it is then return cached data,
   * otherwise use callback. If it fails and if it is within callback error timeout period
   * return cached data. On a successful callback result update the cache
   * @param key cache key
   * @param sourceOfTruthSupplier the data supplier, e.g API call
   * @return cached data
   */
  public T getWithFailover(String key, Supplier<T> sourceOfTruthSupplier) {
    throw new NotImplementedException("Coming soon");
  }
  
  /**
   * Cache data on a successful callback result
   * @param key cache key
   * @param data cache data
   */
  private void cacheData(String key, T data) {
    throw new NotImplementedException("Coming soon");
  }
}
