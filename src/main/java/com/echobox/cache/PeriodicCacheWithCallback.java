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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Serializable;

/**
 * Periodically executes the callback to get the latest value.
 * If it's within the specified period then get the cached value.
 * If the callback fails still get the cached value and update the cache next time
 * @param <T> return type of cache object
 * @author Daniyar
 */
public class PeriodicCacheWithCallback<T extends Serializable> {
  
  private final int callbackPeriod;
  
  private final int callbackErrorTimeout;
  
  private final CacheCallback callback;
  
  private final CacheCheckPeriodic cacheCheckPeriodic;
  
  private final CacheService cacheService;
  
  private final TypeToken<T> returnType;
  
  /**
   * Constructor
   *
   * @param cacheService the cache service
   * @param key cache key
   * @param returnType return type
   * @param cachePeriod period in seconds to check cache
   * @param callbackPeriod period in seconds to check remote data. Also used as cache expiry time
   * @param callbackTimeout timeout in seconds if callback error occurs during which use old data
   * @param callback the callback to get the latest data
   */
  public PeriodicCacheWithCallback(CacheService cacheService, String key, TypeToken<T> returnType,
      int cachePeriod, int callbackPeriod, int callbackTimeout, CacheCallback callback) {
    this.returnType = returnType;
    this.callbackPeriod = callbackPeriod;
    this.callbackErrorTimeout = callbackTimeout;
    this.callback = callback;
    this.cacheService = cacheService;
    this.cacheCheckPeriodic = new CacheCheckPeriodic(cacheService, key, returnType, cachePeriod);
  }
  
  /**
   * Check if it is within callback period. If it is then return cached data,
   * otherwise use callback. If it fails and if it is within callback error timeout period
   * return cached data. On a successful callback result update the cache
   * @return cached data
   */
  public T get() {
    throw new NotImplementedException();
  }
  
  /**
   * Cache data on a successful callback result
   * @param key cache key
   * @param data cache data
   */
  private void cacheData(String key, T data) {
    throw new NotImplementedException();
  }
  
  public interface CacheCallback<T extends Serializable, E extends Exception> {
    T onCall() throws E;
  }
}
