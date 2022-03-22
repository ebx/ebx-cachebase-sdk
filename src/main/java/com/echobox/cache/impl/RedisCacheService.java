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

import com.echobox.shutdown.ShutdownMonitor;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * A default implementation of CacheService which uses Redis as a cache provider
 *
 * @author MarcF
 */
public class RedisCacheService extends RedisCacheServiceBase<String> {
  
  /**
   * A static GSON instance
   */
  public static Gson GSON = new Gson();
  
  /**
   * The singleton instance of this CacheService
   */
  private static volatile RedisCacheService instance;
  
  private RedisCacheService(final Supplier<Long> unixTimeSupplier) {
    super(unixTimeSupplier);
  }
  
  /**
   * Initialise a new RedisCacheService using defaults for cacheClusterEndPoint
   * and cacheClusterPort if either is null.  
   * 
   * The CacheService may only be initialised once.
   * 
   * @param cacheClusterEndPoint the cache cluster end point
   * @param cacheClusterPort the cache cluster port
   * @param shutdownMonitor The shutdown monitor we use to ensure that logging is logged 
   * correctly on shutdown
   */
  public static void initialise(String cacheClusterEndPoint, Integer cacheClusterPort, 
      ShutdownMonitor shutdownMonitor) {
    ((RedisCacheService) getInstance())
        .init(cacheClusterEndPoint, cacheClusterPort, shutdownMonitor);
  }

  /**
   * Get an instance
   * @return The singleton instance
   */
  public static RedisCacheService getInstance() {
    return getInstance(() -> (long) (System.currentTimeMillis() / 1000L));
  }
  
  /**
   * Get an instance
   * @param unixTimeSupplier Supplier to provide current unix times
   * @return The singleton instance
   */
  public static RedisCacheService getInstance(Supplier<Long> unixTimeSupplier) {
    if (instance == null) {
      synchronized (LOCKER) {
        if (instance == null) {
          instance = new RedisCacheService(unixTimeSupplier);
        }
      }
    }
    
    return instance;
  }
  
  /**
   * Shut down this CacheService
   */
  public static void shutdown() {
    ((RedisCacheService) getInstance()).destroy();
  }
  
  @Override
  protected RedisCodec<String, String> createCodec() {
    return StringCodec.UTF8;
  }
  
  @Override
  protected String serialise(Serializable object) {
    // Objects must be plain strings in the cache to run LUA scripts (increment functions)
    if (object instanceof  String) {
      return (String) object;
    }
    return GSON.toJson(object);
  }
  
  @Override
  @SuppressWarnings("unchecked")
  protected <T> T deserialise(String object, TypeToken<T> typeToken) {
    // Objects must be plain strings in the cache to run LUA scripts (increment functions)
    if (typeToken.getRawType().isAssignableFrom(String.class)) {
      return (T) object;
    }
    return GSON.fromJson(object, typeToken.getType());
  }
  
}
