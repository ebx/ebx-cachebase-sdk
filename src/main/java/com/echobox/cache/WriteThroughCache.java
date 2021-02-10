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
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Write-through cache implementation for the {@link CacheService}, this will cache the data 
 * whether available or not. When no data is found it will cache an empty value.
 *
 * @param <T>  the type parameter, this must have overridden 'equals' method so that the cache can
 * check for the existence of a 'noDataValue' or else define a 'isNoDataValue' predicate.
 * @author eddspencer
 */
public class WriteThroughCache<T extends Serializable> {
  
  /**
   * The Cache expiry secs.
   */
  protected final int cacheExpirySecs;
  
  /**
   * The Cache service.
   */
  protected final CacheService cacheService;
  
  /**
   * The Type token for data in the cache.
   */
  protected final TypeToken<T> cachedTypeToken;
  
  /**
   * The data to store when there is no data found in persistence layer
   */
  protected final T noDataValue;
  
  /**
   * Test for whether value is the no data value, so we do not have to rely on Object.equals
   */
  protected final Predicate<T> isNoDataValue;
  
  /**
   * Instantiates a new {@link WriteThroughCache}
   *
   * @param cacheExpirySecs the seconds an object will be cached for before expiring
   * @param cacheService the cache service 
   * @param typeToken the type token for cached data type
   * @param noDataValue value to store in cache when there is no data found in persistence layer,
   * this must have overridden 'equals' method so that the cache can check for the existence of a
   * 'noDataValue' or else define a 'isNoDataValue' predicate.
   */
  public WriteThroughCache(int cacheExpirySecs, CacheService cacheService, TypeToken<T> typeToken,
      T noDataValue) {
    this(cacheExpirySecs, cacheService, typeToken, noDataValue, noDataValue::equals);
  }
  
  /**
   * Instantiates a new {@link WriteThroughCache}
   *
   * @param cacheExpirySecs the seconds an object will be cached for before expiring
   * @param cacheService the cache service  
   * @param typeToken the type token for cached data type
   * @param noDataValue value to store in cache when there is no data found in persistence layer,
   * the cache will check for the existence of a 'noDataValue' using the 'isNoDataValue' predicate.
   * @param isNoDataValue predicate for checking cached no data value
   */
  public WriteThroughCache(int cacheExpirySecs, CacheService cacheService, TypeToken<T> typeToken,
      T noDataValue, Predicate<T> isNoDataValue) {
    if (noDataValue == null) {
      throw new IllegalArgumentException("noDataValue cannot be null");
    }
    this.cacheExpirySecs = cacheExpirySecs;
    this.cacheService = cacheService;
    this.cachedTypeToken = typeToken;
    this.noDataValue = noDataValue;
    this.isNoDataValue = isNoDataValue;
  }
  
  /**
   * Gets the data from the cache if it exists, if not will fetch the data from the provided 
   * supplier. Writes to the cache whether the data exists or not so will only make one call to 
   * supplier during the expiry time.
   *
   * @param <E>  the exception type parameter 
   * @param key cache key to use  
   * @param dataSupplier the data supplier if item is not in cache 
   * @return the data as optional
   * @throws E the exception on getting data
   */
  public <E extends Exception> Optional<T> getAndCacheData(String key,
      DataSupplier<T, E> dataSupplier) throws E {
    final T cachedData = getDataFromCache(key);
  
    if (isNoDataValue.test(cachedData)) {
      return Optional.empty();
    } else if (cachedData != null) {
      return Optional.of(cachedData);
    }
    
    final Optional<T> opData = dataSupplier.get();
    cacheData(key, opData.orElse(noDataValue));
    return opData;
  }
  
  /**
   * Persists the data then stores the data returned by the 'dataPersistor' in the cache if 
   * successful, this data cannot be null. N.B. if there is an exception whilst persisting data 
   * nothing is cached.
   *
   * @param <E>  the exception type parameter 
   * @param key key to use   
   * @param dataPersistor to persist and fetch item to cache
   * @throws E the exception on persisting data
   * @throws IllegalStateException the data persisted was null
   */
  public <E extends Exception> void persistAndUpdateCache(String key,
      DataPersistor<T, E> dataPersistor) throws E {
    final T data = dataPersistor.persist();
    if (data == null) {
      throw new IllegalStateException("Unexpected null data persisted for key = " + key);
    }
    cacheData(key, data);
  }
  
  protected T getDataFromCache(String key) {
    return cacheService.tryGetCachedItem(key, cachedTypeToken);
  }
  
  protected void cacheData(String key, T data) {
    if (cacheService.isCacheAvailable()) {
      cacheService.trySaveItemToCache(key, cacheExpirySecs, data);
    }
  }
  
  public interface DataSupplier<T, E extends Exception> {
    Optional<T> get() throws E;
  }
  
  public interface DataPersistor<T, E extends Exception> {
    T persist() throws E;
  }
  
}
