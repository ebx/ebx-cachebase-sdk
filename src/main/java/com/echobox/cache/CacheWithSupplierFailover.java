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

import com.echobox.time.UnixTime;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Periodically executes the callback to get the latest value.
 * If it's within the specified period then get the cached value.
 * If the callback fails still get the cached value and update the cache next time
 * @param <T> return type of cache object
 * @author Daniyar
 */
public class CacheWithSupplierFailover<T extends Serializable> {
  
  private static final Logger logger = LoggerFactory.getLogger(CacheWithSupplierFailover.class);
  
  private final int defaultCacheSecs;
  
  private final int maxCacheSecsOnError;
  
  private final CacheService cacheService;
  
  private final TypeToken<T> returnType;
  
  // Volatile to ensure if this class is called from multiple threads they all see correct value
  private volatile long lastTimeStampReadFromSourceOfTruth;
  
  // Last time error occurred during the call to source of truth
  private volatile long lastTimeStampSourceOfTruthError;
  
  private volatile T value;
  
  private boolean isInErrorState;
  
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
    this.lastTimeStampReadFromSourceOfTruth = 0L; // Initialise to 0 so
    // always get the data from source of truth first time
    this.isInErrorState = false;
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
    // Only use the supplier once every period, otherwise use cached value
    if (UnixTime.now() - defaultCacheSecs > lastTimeStampReadFromSourceOfTruth) {
      try {
        value = sourceOfTruthSupplier.get();
        cacheData(key, value);
        lastTimeStampReadFromSourceOfTruth = UnixTime.now();
        isInErrorState = false;
      } catch (Exception exception) {
        if (isInErrorState) {
          long interval = UnixTime.now() - lastTimeStampSourceOfTruthError;
          if (interval > maxCacheSecsOnError) {
            // maximum error interval has reached
            String message = String.format("We could not get the value from the source"
                    + " of truth for %d seconds. The maximum wait interval is %d. Giving up",
                interval, maxCacheSecsOnError);
            logger.error(message, exception);
            throw new IllegalStateException(message, exception);
          }
        } else {
          // first time error from source of truth. Use cached value
          value = cacheService.tryGetCachedItem(key, returnType);
          isInErrorState = true;
          lastTimeStampSourceOfTruthError = UnixTime.now();
        }
      }
    } else {
      value = cacheService.tryGetCachedItem(key, returnType);
    }
    return value;
  }
  
  /**
   * Cache data on a successful callback result
   * @param key cache key
   * @param data cache data
   */
  private void cacheData(String key, T data) {
    if (cacheService.isCacheAvailable()) {
      cacheService.trySaveItemToCache(key, defaultCacheSecs, data);
    }
  }
}
