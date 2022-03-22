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
import com.google.gson.reflect.TypeToken;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * A default implementation of CacheService which uses Redis as a cache provider
 *
 * @author MarcF
 */
public class RedisCacheServiceAsBytes extends RedisCacheServiceBase<byte[]> {
  
  /**
   * Charset to use when serialising strings
   */
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  
  /**
   * The singleton instance of this CacheService
   */
  private static volatile RedisCacheServiceAsBytes instance;
  
  private RedisCacheServiceAsBytes(final Supplier<Long> unixTimeSupplier) {
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
    ((RedisCacheServiceAsBytes) getInstance())
        .init(cacheClusterEndPoint, cacheClusterPort, shutdownMonitor);
  }

  /**
   * Get an instance
   * @return The singleton instance
   */
  public static RedisCacheServiceAsBytes getInstance() {
    return getInstance(() -> (long) (System.currentTimeMillis() / 1000L));
  }
  
  /**
   * Get an instance
   * @param unixTimeSupplier Supplier to provide current unix times
   * @return The singleton instance
   */
  public static RedisCacheServiceAsBytes getInstance(Supplier<Long> unixTimeSupplier) {
    if (instance == null) {
      synchronized (LOCKER) {
        if (instance == null) {
          instance = new RedisCacheServiceAsBytes(unixTimeSupplier);
        }
      }
    }
    
    return instance;
  }
  
  /**
   * Shut down this CacheService
   */
  public static void shutdown() {
    ((RedisCacheServiceAsBytes) getInstance()).destroy();
  }
  
  @Override
  protected RedisCodec<byte[], byte[]> createCodec() {
    return ByteArrayCodec.INSTANCE;
  }
  
  @Override
  protected byte[] serialise(Serializable object) {
    // In order to use eval scripts we need to be converting simple numbers to strings
    if (object instanceof String || object instanceof Long || object instanceof Integer) {
      return object.toString().getBytes(CHARSET);
    }
    return SerializationUtils.serialize(object);
  }
  
  @Override
  @SuppressWarnings("unchecked")
  protected <T> T deserialise(byte[] object, TypeToken<T> typeToken) {
    // In order to use eval scripts we need to be converting simple numbers to strings
    if (typeToken.getRawType().isAssignableFrom(String.class)) {
      return (T) CHARSET.decode(ByteBuffer.wrap(object)).toString();
    } else if (typeToken.getRawType().isAssignableFrom(Integer.class)) {
      return (T) Integer.decode(CHARSET.decode(ByteBuffer.wrap(object)).toString());
    } else if (typeToken.getRawType().isAssignableFrom(Long.class)) {
      return (T) Long.decode(CHARSET.decode(ByteBuffer.wrap(object)).toString());
    }
    
    final Object data = SerializationUtils.deserialize(object);
    return (T) typeToken.getRawType().cast(data);
  }
}
