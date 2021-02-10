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

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Write-through cache implementation for the {@link CacheService}, this will cache the data as a
 * byte array whether available or not. When no data is found it will cache an empty value.
 *
 * @param <T>  the type parameter
 * @author eddspencer
 */
public class WriteThroughCacheAsBytes<T extends Serializable> extends WriteThroughCache<T> {
  
  /**
   * Instantiates a new {@link WriteThroughCacheAsBytes}
   *
   * @param cacheExpirySecs the seconds an object will be cached for before expiring
   * @param cacheService the cache service 
   * @param noDataValue value to store in cache when there is no data found in persistence layer,
   * this must have overridden 'equals' method so that the cache can check for the existence of a
   * 'noDataValue' or else define a 'isNoDataValue' predicate.
   */
  public WriteThroughCacheAsBytes(int cacheExpirySecs, CacheService cacheService, T noDataValue) {
    super(cacheExpirySecs, cacheService, new TypeToken<T>() {}, noDataValue);
  }
  
  /**
   * Instantiates a new {@link WriteThroughCacheAsBytes}
   *
   * @param cacheExpirySecs the seconds an object will be cached for before expiring
   * @param cacheService the cache service  
   * @param noDataValue value to store in cache when there is no data found in persistence layer,
   * the cache will check for the existence of a 'noDataValue' using the 'isNoDataValue' predicate.
   * @param isNoDataValue predicate for checking cached no data value
   */
  public WriteThroughCacheAsBytes(int cacheExpirySecs, CacheService cacheService, T noDataValue,
      Predicate<T> isNoDataValue) {
    super(cacheExpirySecs, cacheService, new TypeToken<T>() {}, noDataValue, isNoDataValue);
  }
  
  @Override
  protected T getDataFromCache(String key) {
    return (T) cacheService.tryGetCachedItemFromBytes(key, cachedTypeToken);
  }
  
  @Override
  protected void cacheData(String key, T data) {
    if (cacheService.isCacheAvailable()) {
      final byte[] bytes = cacheService.toBytes(data);
      cacheService.trySaveItemToCache(key, cacheExpirySecs, bytes);
    }
  }
}
