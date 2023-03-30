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
import com.echobox.shutdown.ShutdownMonitor;
import com.google.gson.reflect.TypeToken;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * A default implementation of CacheService which uses Redis as a cache provider, this can be 
 * expended to allow different type of serialisation
 *
 * @param <S>  the serialisation type parameter
 * @author MarcF
 */
abstract class RedisCacheServiceBase<S> extends CacheService {
  
  /**
   * Default topology refresh period
   */
  protected static final long DEFAULT_TOPOLOGY_REFRESH_SECS = TimeUnit.MINUTES.toSeconds(1);
  
  private static final Logger logger = LoggerFactory.getLogger(RedisCacheServiceBase.class);
  
  /**
   * A locker for keeping things thread safe
   */
  protected static final Object LOCKER = new Object();
  
  /**
   * The cache cluster end point. We set a default.
   */
  protected String cacheClusterEndPoint = "127.0.0.1";
  
  /**
   * The cache cluster port. We set a default.
   */
  protected int cacheClusterPort = 6379;
  
  /**
   * The base cluster client
   */
  protected RedisClusterClient clusterClient;
  
  /**
   * The connection pool
   */
  protected GenericObjectPool<StatefulRedisClusterConnection<S, S>> conPool;
  
  protected abstract RedisCodec<S, S> createCodec();
  
  protected abstract S serialise(Serializable object);
  
  protected abstract <T> T deserialise(S object, TypeToken<T> typeToken);
  
  protected RedisCacheServiceBase(final Supplier<Long> unixTimeSupplier) {
    super(TimeoutException.class, unixTimeSupplier);
  }

  /**
   * Initialise a new RedisCacheService using defaults for cacheClusterEndPoint
   * and cacheClusterPort if either is null.
   *
   * The CacheService may only be initialised once.
   *
   * @param cacheClusterEndPoint the cache cluster end point
   * @param cacheClusterPort the cache cluster port
   * @param topologyPeriodicRefreshSeconds the topology periodic refresh seconds
   * @param shutdownMonitor The shutdown monitor we use to ensure that logging is logged
   * correctly on shutdown
   */
  protected void init(String cacheClusterEndPoint, Integer cacheClusterPort,
      Long topologyPeriodicRefreshSeconds, ShutdownMonitor shutdownMonitor) {

    if (shutdownMonitor == null) {
      throw new IllegalArgumentException("RedisCacheService cannot be "
          + "initialised with a null shutdownMonitor.");
    }

    synchronized (LOCKER) {
      // We can only initialise once
      if (initialised) {
        throw new IllegalStateException("The cache factory can only be initialised once.");
      }
      
      RedisCacheServiceBase.shutdownMonitor = shutdownMonitor;
      
      // Overwrite the default cache endpoint if details are provided
      if (cacheClusterEndPoint != null && cacheClusterPort != null) {
        this.cacheClusterEndPoint = cacheClusterEndPoint;
        this.cacheClusterPort = cacheClusterPort;
      }
      
      clusterClient = RedisClusterClient
          .create(RedisURI.create(this.cacheClusterEndPoint, this.cacheClusterPort));
      
      //Don't validate connections to individual nodes as we may connect to them using private ips
      ClusterTopologyRefreshOptions refreshOptions =
          ClusterTopologyRefreshOptions.builder().enablePeriodicRefresh(true).refreshPeriod(
              Duration.ofSeconds(topologyPeriodicRefreshSeconds)).build();
      
      ClusterClientOptions options =
          ClusterClientOptions.builder().validateClusterNodeMembership(false)
              .topologyRefreshOptions(refreshOptions).build();
      clusterClient.setOptions(options);
      
      conPool = ConnectionPoolSupport
          .createGenericObjectPool(() -> clusterClient.connect(createCodec()),
              new GenericObjectPoolConfig<>());
      conPool.setMaxWaitMillis(DEFAULT_CACHE_TIMEOUT_SECS * 1000);
      conPool.setMaxTotal(DEFAULT_CACHE_MAX_TOTAL_OBJECTS);
      conPool.setMaxIdle(DEFAULT_CACHE_MAX_IDLE_OBJECTS);
      conPool.setTimeBetweenEvictionRunsMillis(DEFAULT_TIME_BETWEEN_EVICTION_RUNS_SECS * 1000);
      conPool.setTestWhileIdle(DEFAULT_TEST_WHEN_IDLE);
      conPool.setTestOnBorrow(DEFAULT_TEST_ON_BORROW);
      
      initialised = true;
    }
  }
  
  protected void destroy() {
    synchronized (LOCKER) {
      if (initialised) {
        try {
          if (clusterClient != null) {
            conPool.close();
            clusterClient.shutdown();
          }
        } catch (Exception ex) {
          logger.error("Failed to shutdown redis client.", ex);
        }
        initialised = false;
      }
    }
  }
  
  @Override
  protected Object getRawCachedItem(String key) throws Exception {
    RedisFuture<S> future = null;
    try (StatefulRedisClusterConnection<S, S> conn = conPool.borrowObject()) {
      future = conn.async().get(serialise(key));
      return future.get(DEFAULT_CACHE_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (Exception e) {
      if (future != null) {
        future.cancel(false);
      }
      throw e;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <T> T attemptRawResultCast(Object rawCacheObject, TypeToken<T> typeToken) {
    return (T) deserialise((S) rawCacheObject, typeToken);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean tryDeleteItemFromCache(String key) {

    // Save this in the cache where possible
    if (isCacheAvailable()) {
      try (StatefulRedisClusterConnection<S, S> conn = conPool.borrowObject()) {
        RedisFuture<Long> future = conn.async().del(serialise(key));
        Long result = future.get(DEFAULT_CACHE_TIMEOUT_SECS, TimeUnit.SECONDS);
        return result != null && result > 0;
      } catch (Exception ex) {
        logger.error("Failed to delete cache item using key " + key, ex);
      }
    }

    return false; 
  }

  @Override
  public boolean trySaveItemToCache(String key, int cacheExpiryTimeSeconds, Object item) {
    cacheExpiryTimeSeconds = ensureRelativeSeconds(cacheExpiryTimeSeconds);
    optionalExpiryTimeLogging(key, cacheExpiryTimeSeconds);
    SetArgs args = new SetArgs().ex(cacheExpiryTimeSeconds);
    return redisSet(key, (Serializable) item, args);   
  }

  @Override
  public boolean tryAddItemToCache(String key, int cacheExpiryTimeSeconds, Object item) {
    cacheExpiryTimeSeconds = ensureRelativeSeconds(cacheExpiryTimeSeconds);
    optionalExpiryTimeLogging(key, cacheExpiryTimeSeconds);
    SetArgs args = new SetArgs().ex(cacheExpiryTimeSeconds).nx();
    return redisSet(key, (Serializable) item, args);    
  }
  
  /**
   * Handle the internal difference between save and add required by the interface
   * @param key The unencoded string key for the cache item
   * @param item The unencoded object
   * @param args
   * @return
   */
  private boolean redisSet(String key, Serializable item, SetArgs args) {
    
    if (item == null) {
      throw new IllegalArgumentException("Cache item must be serializable.");
    }
    
    // Save this in the cache where possible
    if (isCacheAvailable()) {
      return performSaveOperation(key, (conn) -> {
        RedisFuture<String> future = null;
        try {
          S itemToCache = serialise(item);
          future = conn.async().set(serialise(key), itemToCache, args);
          String resp = future.get(DEFAULT_CACHE_TIMEOUT_SECS, TimeUnit.SECONDS);
          
          //OK is returned for a successful operation
          return "OK".equals(resp);
        } catch (Exception e) {
          if (future != null) {
            future.cancel(false);
          }
          throw e;
        }
      }).orElse(false);
    }

    return false;
  }

  /**
   * Functional interface that allows us to abstract some of the duplicated code in the save
   * operations.
   * @param <T>
   * @param <R>
   */
  @FunctionalInterface
  private interface RedisSaveFunction<T, R> {
    R apply(T input) throws Exception;
  }
  
  /**
   * Perform the specific part of the save operation. This method wraps the error handling
   * required around the save.
   * @param <T>
   * @param key The key taking part in the operation.
   * @param innerOp The operation that should be performed on StatefulRedisClusterConnection
   * @return
   */
  private <T> Optional<T> performSaveOperation(String key,
      RedisSaveFunction<StatefulRedisClusterConnection<S, S>, T> innerOp) {
    
    // We need to perform the get explicitly so that we can have a timeout
    long startTime = unixTimeSupplier.get();
    try (StatefulRedisClusterConnection<S, S> conn = conPool.borrowObject()) {
      return Optional.ofNullable(innerOp.apply(conn));
    } catch (Exception ex) {
      handleException(key, startTime, ex, DEFAULT_CACHE_TIMEOUT_SECS, "save");
    }
    
    return Optional.empty();
  }
  
  /**
   * Increment LUA script used by tryToIncrementOrDecrementCounterInCacheIfBelowLimit
   */
  private static final String INCR_TO_LIMIT = 
        "local r = redis.call('GET', KEYS[1]) "
      + "if r then "
           //If the key exists and is less than the provided limit
      + "  if tonumber(r) < tonumber(ARGV[1]) then "
             //Increment by one, extend expiry and return TRUE
      + "    redis.call('INCRBY', KEYS[1], 1) "
      + "    redis.call('EXPIRE', KEYS[1], ARGV[2])"
      + "    return true "
      + "  else "
             //Extend the expiry and return FALSE
      + "    redis.call('EXPIRE', KEYS[1], ARGV[2])"
      + "    return false "
      + "  end "
      + "else "
           //If the key does NOT exist increment by one and set expiry
      + "  redis.call('INCRBY', KEYS[1], 1) "
      + "  redis.call('EXPIRE', KEYS[1], ARGV[2])"
      + "  return true "
      + "end ";  

  /**
   * Decrement LUA script used by tryToIncrementOrDecrementCounterInCacheIfBelowLimit
   */
  private static final String DECREMENT_TO_ZERO = 
        "local r = redis.call('GET', KEYS[1]) "
      + "if r then "
           //If the key exists and is greater than zero
      + "  if tonumber(r) > 0 then "
             //Decrement by one, extend expiry and return TRUE
      + "    redis.call('INCRBY', KEYS[1], -1) "
      + "    redis.call('EXPIRE', KEYS[1], ARGV[2])"
      + "    return true "
      + "  else "
             //Extend the expiry and return FALSE
      + "    redis.call('EXPIRE', KEYS[1], ARGV[2])"
      + "    return false "
      + "  end "
      + "else "
           //If the key does NOT exist there is nothing to decrement, return FALSE
      + "  return false "
      + "end ";
  
  @Override
  @SuppressWarnings("unchecked")
  public boolean tryToIncrementOrDecrementCounterInCacheIfBelowLimit(String key,
      int cacheExpiryTimeSeconds, long limit, boolean decrement) {

    // Save this in the cache where possible
    if (isCacheAvailable()) {
      cacheExpiryTimeSeconds = ensureRelativeSeconds(cacheExpiryTimeSeconds);
      optionalExpiryTimeLogging(key, cacheExpiryTimeSeconds);
      
      String limitStr = Long.toString(limit);
      String expirySecsStr = Integer.toString(cacheExpiryTimeSeconds);

      return performSaveOperation(key, (conn) -> {
        RedisFuture<Boolean> future = null;
        try {
          String script = (decrement ? DECREMENT_TO_ZERO : INCR_TO_LIMIT);
          future = conn.async()
              .eval(script, ScriptOutputType.BOOLEAN, (S[]) new Object[]{ serialise(key) },
                  serialise(limitStr), serialise(expirySecsStr));
          
          //Wait for the transaction to complete
          Boolean result = future.get(DEFAULT_CACHE_TIMEOUT_SECS, TimeUnit.SECONDS);

          //Return the response based on the increment command result
          return result;          
        } catch (Exception e) {
          if (future != null) {
            future.cancel(false);
          }
          throw e;
        }
      }).orElse(false);
    }
    
    // Return false if we've not managed to increment the counter for any reason
    return false;
  }

  /**
   * A LUA script used to increment a key. Expiry is only set on key creation.
   */
  private static final String INCR_LUA = "local r = redis.call('GET', KEYS[1]) "
      + "if r then "
      + "  r = redis.call('INCRBY', KEYS[1], ARGV[1]) "
      + "else "
      + "  r = redis.call('INCRBY', KEYS[1], ARGV[1]) "
      + "  redis.call('EXPIRE', KEYS[1], ARGV[2])"
      + "end "
      + "return r ";
  
  @Override
  @SuppressWarnings("unchecked")
  public long incr(String key, int by, int def, int cacheExpiryTimeSeconds) {
    
    if (def != by) {
      throw new IllegalArgumentException("Redis only supports a 'def' value that is equal "
          + "to 'by'.");
    }

    // Save this in the cache where possible
    if (isCacheAvailable()) {
      cacheExpiryTimeSeconds = ensureRelativeSeconds(cacheExpiryTimeSeconds);
      optionalExpiryTimeLogging(key, cacheExpiryTimeSeconds);
      
      String byStr = Integer.toString(by);
      String expirySecsStr = Integer.toString(cacheExpiryTimeSeconds);
      
      return performSaveOperation(key, (conn) -> {
        RedisFuture<Long> future = null;
        try {
          future = conn.async()
              .eval(INCR_LUA, ScriptOutputType.INTEGER, (S[]) new Object[]{ serialise(key) },
                  serialise(byStr), serialise(expirySecsStr));
          
          //Wait for the transaction to complete
          Long result = future.get(DEFAULT_CACHE_TIMEOUT_SECS, TimeUnit.SECONDS);

          //Return the response based on the increment command result
          return result;
        } catch (Exception e) {
          if (future != null) {
            future.cancel(false);
          }
          throw e;
        }
      }).orElse(-1L);
    }
    
    return -1;
  }
  
  @Override
  public int getNumActiveConnections() {
    return conPool.getNumActive();
  }
  
  /**
   * Primarily for legacy reasons, given we had the memcached implementation first. It
   * interprets any expiry seconds larger than 60*60*24*30 as a unix time. Redis always
   * deals in relative seconds.
   * @param cacheExpiryTimeSeconds
   * @return
   */
  private int ensureRelativeSeconds(int cacheExpiryTimeSeconds) {
    int thirtyDaysInSeconds = 2592000;
    if (cacheExpiryTimeSeconds > thirtyDaysInSeconds) {
      return Math.max(1, cacheExpiryTimeSeconds - unixTimeSupplier.get().intValue());
    } else {
      return cacheExpiryTimeSeconds;
    }
  }
  
  /**
   * Optionally log the expiry time 
   * @param key The logging key that is being used
   * @param cacheExpiryTimeSeconds The expiry time in seconds
   */
  private void optionalExpiryTimeLogging(String key, int cacheExpiryTimeSeconds) {
    if (LOG_WARNING_FOR_ZERO_EXPIRY && cacheExpiryTimeSeconds == 0) {
      logger.warn("Zero cache expiry used for key " + key + ".");
    }

    if (cacheExpiryTimeSeconds > LOG_WARNING_FOR_EXPIRY_LONGER_THAN_SECS) {
      logger.warn("Cache expiry for key " + key + " was " + cacheExpiryTimeSeconds + " secs.");
    }
  }
  
}
