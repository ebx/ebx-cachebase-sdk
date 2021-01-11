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
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.ClientMode;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Locator;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import net.spy.memcached.transcoders.LongTranscoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * A default implementation of CacheService which uses Memcached as a cache provider
 *
 * @author MarcF
 */
public class MemcachedCacheService extends CacheService {
  
  private static Logger logger = LoggerFactory.getLogger(MemcachedCacheService.class);
  
  /**
   * A locker for keeping things thread safe
   */
  private static final Object LOCKER = new Object();

  /**
   * The cache cluster end point. We set a default.
   */
  private static String cacheClusterEndPoint = "127.0.0.1";

  /**
   * The cache cluster port. We set a default.
   */
  private static int cacheClusterPort = 11211;

  /**
   * An optional Memcached client which can act as a caching layer between MySQL
   */
  private static MemcachedClient memcachedClient;
  
  /**
   * The singleton instance of DefaultMemcachedCacheService
   */
  private static volatile MemcachedCacheService instance;

  private MemcachedCacheService(final Supplier<Long> unixTimeSupplier) {
    super(CheckedOperationTimeoutException.class, unixTimeSupplier);
  }

  /**
   * Initialise a new DefaultMemcachedCacheService using defaults for cacheClusterEndPoint
   * and cacheClusterPort if either is null.  
   * 
   * The DefaultMemcachedCacheService may only be initialised once.
   * 
   * @param cacheClusterEndPoint the cache cluster end point
   * @param cacheClusterPort the cache cluster port
   * @param shutdownMonitor The shutdown monitor we use to ensure that logging is logged 
   * correctly on shutdown
   * @throws Exception If an exception is thrown during initialisation
   */
  public static void initialise(String cacheClusterEndPoint,
      Integer cacheClusterPort, ShutdownMonitor shutdownMonitor) throws Exception {

    if (shutdownMonitor == null) {
      throw new IllegalArgumentException("DefaultMemcachedCacheService cannot be "
          + "initialised with a null shutdownMonitor.");
    }

    synchronized (LOCKER) {
      // We can only initialise once
      if (initialised) {
        throw new IllegalStateException("The cache factory can only be initialised once.");
      }

      MemcachedCacheService.shutdownMonitor = shutdownMonitor;

      // Overwrite the default cache endpoint if details are provided
      if (cacheClusterEndPoint != null && cacheClusterPort != null) {
        MemcachedCacheService.cacheClusterEndPoint = cacheClusterEndPoint;
        MemcachedCacheService.cacheClusterPort = cacheClusterPort;
      }

      // The setLocatorType(Locator.CONSISTENT) enables consistent hashing
      // which ensures the cache cluster can scale efficiently if required
      //
      // The client mode depends on if this is a standalone memcached
      // instance or backed by a config cluster. As per the memcached implementation
      // "All config endpoints has ".cfg." subdomain in the DNS name."
      // which require a dynamic client mode
      ConnectionFactoryBuilder connFactoryBuilder =
          new ConnectionFactoryBuilder().setLocatorType(Locator.CONSISTENT);
      if (MemcachedCacheService.cacheClusterEndPoint.contains(".cfg.")) {
        connFactoryBuilder.setClientMode(ClientMode.Dynamic);
      } else {
        connFactoryBuilder.setClientMode(ClientMode.Static);
      }

      //We want to disable the unnecessary background logging that Memcached spits out about
      //configuration poller errors
      System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
      java.util.logging.Logger.getLogger("net.spy.memcached").setLevel(Level.OFF);
      
      memcachedClient = new MemcachedClient(connFactoryBuilder.build(), Collections.singletonList(
          new InetSocketAddress(MemcachedCacheService.cacheClusterEndPoint,
              MemcachedCacheService.cacheClusterPort)));

      initialised = true;
    }
  }

  /**
   * Get an instance
   * @return The singleton instance
   */
  public static MemcachedCacheService getInstance() {
    return getInstance(() -> (long) (System.currentTimeMillis() / 1000L));
  }
  
  /**
   * Get an instance
   * @param unixTimeSupplier Supplier to provide current unix times
   * @return The singleton instance
   */
  public static MemcachedCacheService getInstance(Supplier<Long> unixTimeSupplier) {
    if (instance == null) {
      synchronized (LOCKER) {
        if (instance == null) {
          instance = new MemcachedCacheService(unixTimeSupplier);
        }
      }
    }
    
    return instance;
  }

  /**
   * A getter for the unix time as determined by Memcached
   * @return The unix time as determined by Memcached - the unix times on each of the Memcached 
   * servers should be in sync, but this method averages them to determine a single unix time
   * as seen by Memcached. If not initialised will return -1.
   */
  public static long getMemcachedServerUnixTime() {
    
    if (initialised) {
      // Obtain the stats for each Memcached server
      Map<SocketAddress, Map<String, String>> memcachedServerStats =
          getInstance().getCacheClient().getStats();
  
      // For each memcached server
      long sumOfMemcachedServerUnixTimes = 0;
      int numberOfServers = 0;
      for (Map<String, String> stats : memcachedServerStats.values()) {
        sumOfMemcachedServerUnixTimes = sumOfMemcachedServerUnixTimes
            + Long.parseLong(stats.get("time"));
        numberOfServers++;
      }
      long averageServerUnixTime = sumOfMemcachedServerUnixTimes / numberOfServers;
  
      return averageServerUnixTime;
    } else {
      return -1;
    }
  }
  
  /**
   * Shut down this DefaultMemcachedCacheService
   */
  public static void shutdown() {
    synchronized (LOCKER) {
      if (initialised) {
        try {
          if (memcachedClient != null) {
            memcachedClient.shutdown();
          }
        } catch (Exception ex) {
          logger.error("Failed to shutdown memcached client.", ex);
        }
        initialised = false;
      }
    }
  }

  /**
   * Gets the MemcachedClient
   *
   * @return The global MemcachedClient. This object is thread safe.
   */
  public MemcachedClient getCacheClient() {
    return memcachedClient;
  }
  
  @Override
  protected Object getRawCachedItem(String key) throws Exception {
    Future<Object> future = getCacheClient().asyncGet(key);
    try {
      return future.get(DEFAULT_CACHE_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (Exception e) {
      future.cancel(false);
      throw e;
    }
  }
  
  /* (non-Javadoc)
   * @see com.echobox.cache.CacheService#tryDeleteItemFromCache(java.lang.String)
   */
  @Override
  public boolean tryDeleteItemFromCache(String key) {

    // Save this in the cache where possible
    if (isCacheAvailable()) {
      try {
        getCacheClient().delete(key);
        return true;
      } catch (Exception ex) {
        logger.error("Failed to delete cache item using key " + key, ex);
      }
    }

    return false; 
  }

  /* (non-Javadoc)
   * @see com.echobox.cache.CacheService#trySaveItemToCache(java.lang.String, int, java.lang.Object)
   */
  @Override
  public boolean trySaveItemToCache(String key, int cacheExpiryTimeSeconds, Object item) {

    // Save this in the cache where possible
    if (isCacheAvailable()) {
      optionalExpiryTimeLogging(key, cacheExpiryTimeSeconds);
      
      try {
        // The set is always async unless we want to wait on the returned future
        getCacheClient().set(key, cacheExpiryTimeSeconds, item);
        return true;
      } catch (Exception ex) {
        logger.error("Failed to save cache item using key " + key, ex);
      }
    }

    return false;
  }

  /* (non-Javadoc)
   * @see com.echobox.cache.CacheService#tryAddItemToCache(java.lang.String, int, java.lang.Object)
   */
  @Override
  public boolean tryAddItemToCache(String key, int cacheExpiryTimeSeconds, Object item) {

    // Save this in the cache where possible
    if (isCacheAvailable()) {
      optionalExpiryTimeLogging(key, cacheExpiryTimeSeconds);
      
      try {
        return getCacheClient().add(key, cacheExpiryTimeSeconds, item).get();
      } catch (Exception ex) {
        logger.error("Failed to save cache item using key " + key, ex);
      }
    }
    
    return false;
  }
  
  /* (non-Javadoc)
   * @see com.echobox.cache.CacheService#tryToIncrementOrDecrementCounterInCacheIfBelowLimit(
   * java.lang.String, int, long, boolean)
   */
  @Override
  public boolean tryToIncrementOrDecrementCounterInCacheIfBelowLimit(String key,
      int cacheExpiryTimeSeconds, long limit, boolean decrement) {

    // Save this in the cache where possible
    if (isCacheAvailable()) {
      optionalExpiryTimeLogging(key, cacheExpiryTimeSeconds);
      
      try {

        CASValue<Object> existingValueWithId = getCacheClient().gets(key);
        if (existingValueWithId == null) {
          boolean counterInitialised = getCacheClient().add(key, cacheExpiryTimeSeconds, 0L).get();
          if (counterInitialised) {
            // If we've managed to initialise the counter, try again
            return tryToIncrementOrDecrementCounterInCacheIfBelowLimit(key, cacheExpiryTimeSeconds,
                limit, decrement);
          } else {
            // We haven't managed to initialise the counter ( perhaps another thread has done so).
            // So check again for existence of the counter, throwing an exception if we
            // still have no initialised counter
            existingValueWithId = getCacheClient().gets(key);
            if (existingValueWithId == null) {
              throw new IllegalStateException("Value for counter in the cache is null "
                  + " and we have not been able to add the value");
            }
          }
        }

        // The counter is not null, below the limit, so try to increment it if it hasn't been
        // changed by another thread
        LongTranscoder longTranscoder = new LongTranscoder();

        // If we end up here, we have a value for the counter in the cache.
        // Obtain the value
        Long existingValue = (Long) existingValueWithId.getValue();
        // Double check the counter isn't null
        if (existingValue == null) {
          throw new IllegalStateException("Unexpected null existing value in cache for key:" + key);
        } else {
          // If the existing counter is not null and we are incrementing, return false if
          // the value is at the limit or above
          if (!decrement && existingValue.longValue() >= limit) {
            // Reset the expiration of the existing value
            getCacheClient().cas(key, existingValueWithId.getCas(), cacheExpiryTimeSeconds,
                existingValue.longValue(), longTranscoder);
            return false;
          }
          // If we are decrementing and the existing value is 0 or below return false
          if (decrement && existingValue.longValue() <= 0) {
            // Reset the expiration of the existing value
            getCacheClient().cas(key, existingValueWithId.getCas(), cacheExpiryTimeSeconds,
                existingValue.longValue(), longTranscoder);
            return false;
          }
        }

        // Compare and set operation - attempts to increment the counter if it hasn't already
        // been changed.
        long delta = decrement ? -1 : 1;
        CASResponse response = getCacheClient()
            .cas(key, existingValueWithId.getCas(), cacheExpiryTimeSeconds,
                existingValue.longValue() + delta, longTranscoder);
        // Return true if we've managed to increment, false otherwise
        return response.equals(CASResponse.OK);
      } catch (Exception ex) {
        logger.error("Failed to modify counter using " + key, ex);
      }
    }
    
    // Return false if we've not managed to increment the counter for any reason
    return false;
  }

  /* (non-Javadoc)
   * @see com.echobox.cache.CacheService#incr(java.lang.String, int, int, int)
   */
  @Override
  public long incr(String key, int by, int def, int cacheExpiryTimeSeconds) {
    if (isCacheAvailable()) {
      optionalExpiryTimeLogging(key, cacheExpiryTimeSeconds);
      return getCacheClient().incr(key, by, def, cacheExpiryTimeSeconds);
    } else {
      return -1;
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
    
    int seconds = cacheExpiryTimeSeconds;
    
    //Seconds greater than 30 days should be interpreted as unix times
    boolean isExpiryUnixTime = cacheExpiryTimeSeconds > 3600 * 24 * 30;
    if (isExpiryUnixTime) {
      seconds = cacheExpiryTimeSeconds - unixTimeSupplier.get().intValue();
    }
    
    if (seconds > LOG_WARNING_FOR_EXPIRY_LONGER_THAN_SECS) {
      logger.warn("Cache expiry for key " + key + " was " + seconds + " secs.");
    }
  }
}
