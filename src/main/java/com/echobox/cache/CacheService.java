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

import com.echobox.shutdown.ShutdownMonitor;
import com.google.common.base.Ticker;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.SerializationUtils;
import org.isomorphism.util.TokenBucket;
import org.isomorphism.util.TokenBuckets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Encapsulates the interface we need from a Cache implementation
 * for many common Echobox services
 * 
 * @author Michael Lavelle
 */
public abstract class CacheService {
  
  private static Logger logger = LoggerFactory.getLogger(CacheService.class);
  
  /**
   * If true and a cache expiry of zero is provided log a warning
   */
  public static volatile boolean LOG_WARNING_FOR_ZERO_EXPIRY = false; 
  
  /**
   * Any cached values that will expire after this number of seconds will log a warning.
   * Cache expirations provided in unixTimes will be converted to seconds for the purpose of this
   * analysis. 
   */
  public static int LOG_WARNING_FOR_EXPIRY_LONGER_THAN_SECS = Integer.MAX_VALUE;
  
  /**
   * True once the CacheService singleton has been initialised
   */
  protected static volatile boolean initialised = false;

  /**
   * True if the token bucket has no tokens in it
   */
  protected static volatile boolean timeoutBucketEmpty = false;
 
  /**
   * Should be set once this CacheService singleton has been initialised
   */
  protected static volatile ShutdownMonitor shutdownMonitor;

  /**
   * The default number of seconds to wait for the cache to respond
   */
  protected static int DEFAULT_CACHE_TIMEOUT_SECS = 15;
  
  /**
   * The default number of max total objects in the pool - this value must be ideally equal to the
   * DEFAULT_CACHE_MAX_IDLE_OBJECTS to avoid threads being closed just after being initialised
   */
  protected static int DEFAULT_CACHE_MAX_TOTAL_OBJECTS = 8;
  
  /**
   * The default number of max idle objects - this value must be ideally equal to the
   * DEFAULT_CACHE_MAX_TOTAL_OBJECTS to avoid threads being closed just after being initialised
   */
  protected static int DEFAULT_CACHE_MAX_IDLE_OBJECTS = 8;
  
  /**
   * The default time between eviction runs in seconds to sleep between runs of the idle
   * object evictor thread.
   */
  protected static int DEFAULT_TIME_BETWEEN_EVICTION_RUNS_SECS = -1;
  
  /**
   * The default test when idle flag whether objects sitting idle in the pool will be
   * validated by the idle object evictor
   */
  protected static boolean DEFAULT_TEST_WHEN_IDLE = false;
  
  /**
   * The default test on borrow flag where objects borrowed from the pool will be validated before
   * being returned
   */
  protected static boolean DEFAULT_TEST_ON_BORROW = false;

  /**
   * Timeout bucket that will stop the cache from being called when there has been a certain 
   * number of timeouts in a given time period. By default set this bucket to null to disable 
   * this functionality
   */
  protected static TokenBucket timeoutBucket = null;

  /**
   * The class that is thrown on timeout exception
   */
  protected final Class<? extends Exception> classTimeoutException;
  
  /**
   * A supplier for providing unix times
   */
  protected final Supplier<Long> unixTimeSupplier;
  
  /**
   * Default constructor
   *
   * @param classTimeoutException the class timeout exception
   * @param unixTimeSupplier Supplier to provide current unix times
   */
  protected CacheService(final Class<? extends Exception> classTimeoutException,
      final Supplier<Long> unixTimeSupplier) {
    this.classTimeoutException = classTimeoutException;
    this.unixTimeSupplier = unixTimeSupplier;
  }

  /**
   * A getter a default timeout in secs
   * @return The default timeout secs. Defaults to 15 seconds.
   */
  public static int getDefaultCacheTimeoutSecs() {
    return DEFAULT_CACHE_TIMEOUT_SECS;
  }
  
  /**
   * Set the default timeout to the provided value
   * @param newTimeoutSecs The new default timeout seconds
   */
  public static void setDefaultCacheTimeoutSecs(int newTimeoutSecs) {
    DEFAULT_CACHE_TIMEOUT_SECS = newTimeoutSecs;
  }

  /**
   * Set a timeout strategy, which will disable the cache if a given number of timeouts happen in
   * a second. Once this number is reached the period will be increased by the given factor so 
   * that maxTimeoutsPerSecond will be allowed per factorBackOff seconds. If there continue to be
   * a high rate of timeouts then this period will increase exponentially, once the timeouts stop
   * then the initial one second period is reinstated.
   *
   * Whilst the strategy is triggered the entire cache is disabled to stop further cache calls from 
   * occurring and timing out unnecessarily.
   *
   * @param maxTimeoutsPerSecond max allowed timeouts per second
   * @param factorBackOff factor to increase period by when max timeouts are reached
   */
  public static void setTimeoutStrategy(long maxTimeoutsPerSecond, final double factorBackOff) {
    // Back off the refill period when the cache is not available
    timeoutBucket = TokenBuckets.builder().withCapacity(maxTimeoutsPerSecond).withRefillStrategy(
        new ExponentialBackOffRefillStrategy(Ticker.systemTicker(), maxTimeoutsPerSecond,
            factorBackOff, 1, TimeUnit.SECONDS, () -> timeoutBucketEmpty)).build();
  } 
  
  /**
   * Get the default max total objects value
   * @return the default cache max total objects
   */
  public static int getDefaultCacheMaxTotalObjects() {
    return DEFAULT_CACHE_MAX_TOTAL_OBJECTS;
  }
  
  /**
   * Set the default cache max total objects value
   * @param newMaxTotal the new default cache max total value
   */
  public static void setDefaultCacheMaxTotalObjects(int newMaxTotal) {
    DEFAULT_CACHE_MAX_TOTAL_OBJECTS = newMaxTotal;
  }
  
  /**
   * Get the default max idle objects
   * @return the default cache max idle object
   */
  public static int getDefaultCacheMaxIdleObjects() {
    return DEFAULT_CACHE_MAX_IDLE_OBJECTS;
  }
  
  /**
   * Set the default max idle objects
   * @param newMaxIdle the new default cache idle objects
   */
  public static void setDefaultCacheIdleObjects(int newMaxIdle) {
    DEFAULT_CACHE_MAX_IDLE_OBJECTS = newMaxIdle;
  }
  
  /**
   * Get the default time between runs seconds
   * @return the default cache time between eviction runs seconds
   */
  public static int getDefaultTimeBetweenEvictionRunsSecs() {
    return DEFAULT_TIME_BETWEEN_EVICTION_RUNS_SECS;
  }

  /**
   * Set the default time between eviction runs seconds
   * @param newTimeBetweenEvictionRunsSecs the new default time between eviction runs seconds
   */
  public static void setDefaultTimeBetweenEvictionRunsSecs(
      int newTimeBetweenEvictionRunsSecs) {
    DEFAULT_TIME_BETWEEN_EVICTION_RUNS_SECS = newTimeBetweenEvictionRunsSecs;
  }

  /**
   * Get the default test when idle flag
   * @return the default test when idle flag
   */
  public static boolean getDefaultTestWhenIdle() {
    return DEFAULT_TEST_WHEN_IDLE;
  }

  /**
   * Set the default test when idle flag
   * @param newTestWhenIdle the new test when idel flag
   */
  public static void setDefaultTestWhenIdle(boolean newTestWhenIdle) {
    DEFAULT_TEST_WHEN_IDLE = newTestWhenIdle;
  }

  /**
   * Get the default test on borrow flag
   * @return the default test on borrow flag
   */
  public static boolean getDefaultTestOnBorrow() {
    return DEFAULT_TEST_ON_BORROW;
  }

  /**
   * Set the default test on borrow flag
   * @param newTestOnBorrow the new test on borrow flag
   */
  public static void setDefaultTestOnBorrow(boolean newTestOnBorrow) {
    DEFAULT_TEST_ON_BORROW = newTestOnBorrow;
  }
  
  /**
   * Get the configured unixtime supplier in this cache service
   * @return The configured unix time supplier
   */
  public Supplier<Long> getUnixTimeSupplier() {
    return unixTimeSupplier;
  }
  
  /**
   * Return true if this instance is available/initialised
   * @return True if this cache service has been initialised
   */
  public boolean isCacheAvailable() {
    if (initialised && timeoutBucket != null) {
      // If there are no tokens in the timeout bucket then the cache will be unavailable until the 
      // bucket is next refilled.
      timeoutBucketEmpty = timeoutBucket.getNumTokens() <= 0;
      return !timeoutBucketEmpty;
    }
    return initialised;
  }
  
  /**
   * Attempts to increment the numeric value with the specified key. Expiry time is NOT extended 
   * by incrementing an existing value.
   * @param key The key
   * @param by The amount to increment.
   * @param def The default value (after being incremented) (if the counter did not exist before
   * this call)
   * @param cacheExpiryTimeSeconds The expiry time in seconds of this KEY, but only relative
   * to it's instantiation.
   * @return the new value, or -1 if we were unable to increment or add
   */
  public abstract long incr(String key, int by, int def, int cacheExpiryTimeSeconds);

  /**
   * Tries to increment a counter in the cache if it's below a specified limit.
   * Attempts to initialise the counter to 0 if it's not present
   *
   * @param key the object reference in the cache.
   * @param cacheExpiryTimeSeconds The expiry time in seconds of the item in the cache
   * @param limit The max value permitted for the item in cache when incrementing ( inclusive).
   * The min allowed value is zero.
   * @param decrement Whether we are decrementing the counter.  If false, it is assumed we are
   *                  incrementing, if true we are decrementing
   * @return true if the item was successfully incremented, false otherwise,
   */
  public abstract boolean tryToIncrementOrDecrementCounterInCacheIfBelowLimit(String key,
      int cacheExpiryTimeSeconds, long limit , boolean decrement);

  /**
   * Serialise the provided object to byte[] using org.apache.commons.lang3.SerializationUtils
   * @param objectToSerialise The object to serialise
   * @return Serialised bytes
   */
  public byte[] toBytes(Serializable objectToSerialise) {
    return SerializationUtils.serialize(objectToSerialise);
  }
  
  /**
   * Try to retrieve an item that has been serialised using
   * org.apache.commons.lang3.SerializationUtils to byte[] from the cache with the time to
   * expiry using the provided key and type token
   * @param <T> The FINAL type of this item
   * @param key The key
   * @param typeToken The type token of the object being retrieved
   * @return Optional that contains the cached result if it existed
   */
  public <T> Optional<CachedResult<T>> tryGetCachedItemFromBytesWithTtl(String key,
      TypeToken<T> typeToken) {
    
    Optional<CachedResult<byte[]>> cachedResult =
        tryGetCachedItemWithTtl(key, new TypeToken<byte[]>() {});
    
    if (!cachedResult.isPresent()) {
      return Optional.empty();
    }
    
    byte[] cachedBytes = cachedResult.get().getValue();
    if (cachedBytes != null) {
      try {
        T typedResult =
            (T) typeToken.getRawType().cast(SerializationUtils.deserialize(cachedBytes));
        return Optional.of(new CachedResult<>(typedResult, cachedResult.get().getTtl()));
      } catch (Exception ex) {
        logger.error("Failed to cast cached bytes to " + typeToken.toString() + " using key "
            + key + ". Item will be deleted " + "from the cache.", ex);
        tryDeleteItemFromCache(key);
      }
    }
    
    //Return the same as if the cache item does not exist
    return Optional.empty();
  }
  
  /**
   * Try to retrieve an item from the cache with the time to live using the provided key and
   * type token
   * @param <T> The type of this item
   * @param key The key
   * @param typeToken The type token of the object being retrieved
   * @return Optional that contains the cached result if it existed
   */
  public <T> Optional<CachedResult<T>> tryGetCachedItemWithTtl(String key, TypeToken<T> typeToken) {
    // Use a runtime exception set here to ensure no checked exceptions are thrown since we want
    // to use an empty set here but also allow for checked exceptions to be thrown when required
    final Set<Class<? extends RuntimeException>> exceptionsToThrowDirectly = new HashSet<>();
    
    return tryGetCachedItemWithTtl(key, typeToken, exceptionsToThrowDirectly);
  }
  
  /**
   * Try to retrieve an item from the cache with the time to live using the provided key and
   * type token
   * @param <T> The type of this item
   * @param <E> The type of exception to get directly thrown
   * @param key The key
   * @param typeToken The type token of the object being retrieved
   * @param exceptionsToThrowDirectly set of exception types to throw directly
   * @return An optional containing a cached result if it existed
   * @throws E the exception
   */
  @SuppressWarnings("unchecked")
  public <T, E extends Exception> Optional<CachedResult<T>> tryGetCachedItemWithTtl(String key,
      TypeToken<T> typeToken,
      Set<Class<? extends E>> exceptionsToThrowDirectly) throws E {
    validateInputs(key, typeToken, exceptionsToThrowDirectly);
    
    // Check to see if we might be able to access a cached item
    if (isCacheAvailable()) {
      Optional<CachedResult<Object>> rawCachedResult = Optional.empty();
      
      // We need to perform the get explicitly so that we can have a timeout
      long startTime = unixTimeSupplier.get();
      
      try {
        rawCachedResult = getRawCachedItemWithTtl(key);
      } catch (Exception ex) {
        if (exceptionsToThrowDirectly.contains(ex.getClass())) {
          throw (E) ex;
        }
        handleException(key, startTime, ex, getDefaultCacheTimeoutSecs(), "retrieve");
      }
      
      try {
        if (!rawCachedResult.isPresent()) {
          return Optional.empty();
        }
        
        T result = attemptRawResultCast(rawCachedResult.get().getValue(), typeToken);
        return Optional.of(new CachedResult<>(result, rawCachedResult.get().getTtl()));
      } catch (Exception ex) {
        logger.error("Failed to cast cache item to " + typeToken + " using key "
            + key + ". Item will be deleted " + "from the cache.", ex);
        // To ensure the error does not get thrown again for the same key we delete this
        // cache entry
        tryDeleteItemFromCache(key);
        return Optional.empty();
      }
      
    } else {
      return Optional.empty();
    }
  }
  
  /**
   * Attempt to get an uncast Object and the time to live from the cache using the provided key
   * @param key the key
   * @return Optional that contains a cached result if it existed
   * @throws Exception the exception
   */
  protected abstract Optional<CachedResult<Object>> getRawCachedItemWithTtl(String key)
      throws Exception;
  
  /**
   * Try to retrieve an item that has been serialised using 
   * org.apache.commons.lang3.SerializationUtils to byte[] from the cache using the provided 
   * key of the provided final type
   * @param <T> The FINAL type of this item
   * @param key The key
   * @param typeToken The type token of the object being retrieved
   * @return The cached item with this key, or null if no such item exists or the clazz does not
   * an existing object
   */
  public <T> T tryGetCachedItemFromBytes(String key, TypeToken<T> typeToken) {
    
    byte[] cachedBytes = tryGetCachedItem(key, new TypeToken<byte[]>(){});
    if (cachedBytes != null) {
      try {
        return (T) typeToken.getRawType().cast(SerializationUtils.deserialize(cachedBytes));
      } catch (Exception ex) {
        logger.error("Failed to cast cached bytes to " + typeToken.toString() + " using key "
            + key + ". Item will be deleted " + "from the cache.", ex);
        tryDeleteItemFromCache(key);
      }
    }
    
    //Return the same as if the cache item does not exist
    return null;
  }
  
  /**
   * Try to retrieve an item from the cache using the provided key of the provided clazz
   * @param <T> The type of this item
   * @param key The key
   * @param typeToken The type token of the object being retrieved
   * @return The cached item with this key, or null if no such item exists or the clazz does not
   * an existing object
   */
  public <T> T tryGetCachedItem(String key, TypeToken<T> typeToken) {
    // Use a runtime exception set here to ensure no checked exceptions are thrown since we want
    // to use an empty set here but also allow for checked exceptions to be thrown when required
    final Set<Class<? extends RuntimeException>> exceptionsToThrowDirectly = new HashSet<>();

    return tryGetCachedItem(key, typeToken, exceptionsToThrowDirectly);
  }

  /**
   * Try to retrieve an item from the cache using the provided key of the provided clazz
   * @param <T> The type of this item
   * @param <E> The type of exception to get directly thrown
   * @param key The key
   * @param typeToken The type token of the object being retrieved
   * @param exceptionsToThrowDirectly set of exception types to throw directly
   * @return The cached item with this key, or null if no such item exists or the clazz does not
   * an existing object
   * @throws E the exception
   */
  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T tryGetCachedItem(String key, TypeToken<T> typeToken,
      Set<Class<? extends E>> exceptionsToThrowDirectly) throws E {
    validateInputs(key, typeToken, exceptionsToThrowDirectly);

    // Check to see if we might be able to access a cached item
    if (isCacheAvailable()) {
      Object rawCachedResult = null;

      // We need to perform the get explicitly so that we can have a timeout
      long startTime = unixTimeSupplier.get();

      try {
        rawCachedResult = getRawCachedItem(key);
      } catch (Exception ex) {
        if (exceptionsToThrowDirectly.contains(ex.getClass())) {
          throw (E) ex;
        }
        handleException(key, startTime, ex, getDefaultCacheTimeoutSecs(), "retrieve");
      }

      try {
        if (rawCachedResult == null) {
          return null;
        } else {
          return attemptRawResultCast(rawCachedResult, typeToken);
        }
      } catch (Exception ex) {
        logger.error("Failed to cast cache item to " + typeToken.toString() + " using key "
            + key + ". Item will be deleted " + "from the cache.", ex);
        // To ensure the error does not get thrown again for the same key we delete this 
        // cache entry
        tryDeleteItemFromCache(key);
        return null;
      }
      
    } else {
      return null;
    }
  }
  
  private <E extends Exception> void validateInputs(String key, TypeToken<?> typeToken,
      Set<Class<? extends E>> exceptionsToThrowDirectly) {
    if (key == null) {
      throw new IllegalArgumentException("Provided cache key cannot be null.");
    }
    if (typeToken == null) {
      throw new IllegalArgumentException("Provided typeToken cannot be null.");
    }
    if (exceptionsToThrowDirectly == null) {
      throw new IllegalArgumentException("Provided exceptionsToThrowDirectly cannot be null.");
    }
  }
  
  /**
   * Attempt to get an uncast Object from the cache using the provided key
   * @param key the key
   * @return An object if one exists for the provided key
   * @throws Exception the exception
   */
  protected abstract Object getRawCachedItem(String key) throws Exception;

  /**
   * Perform any implementation specific cast operation from the raw result to the desired type.
   * No error handling should be performed in this method.
   *
   * @param <T>            the type of this item
   * @param rawCacheObject The raw result as retrieved from the cache
   * @param typeToken      The type token for the object being cast
   * @return T if the cast was successful, otherwise null
   * @throws Exception Any exception that is thrown will be handled by the calling method
   */
  protected <T> T attemptRawResultCast(Object rawCacheObject, TypeToken<T> typeToken)
      throws Exception {
    //Default implementation that will be suitable for most implementations
    return (T) typeToken.getRawType().cast(rawCacheObject);
  }
  
  /**
   * Save an object to the cache, replacing any existing value for the key
   *
   * @param key the object reference in the cache.
   * @param cacheExpiryTimeSeconds time it will stay in the cache (in seconds)
   * @param item the item to add
   * @return true if the object was successfully saved to the cache, otherwise false.
   */
  public abstract boolean trySaveItemToCache(String key, int cacheExpiryTimeSeconds, Object item);

  /**
   * Tries to add the item to the cache if it doesn't already exist, fails silently if we
   * can't add the item successfully ( eg. if an item with the key already exists )
   * 
   * @param key The key
   * @param cacheExpiryTimeSeconds The expiry time in seconds of this incremented value
   * @param item The value to add to the cache
   * @return Whether the add to the cache was successful, false if we were not able to add 
   * ( eg. an item with the key already exists )
   */
  public abstract boolean tryAddItemToCache(String key, int cacheExpiryTimeSeconds, Object item);
  
  /**
   * Delete an object from the cache.
   * 
   * @param key The cache key to delete
   * @return True if the operation was successfully executed. Note this is different from if the
   * item existed and was actually deleted.
   */
  public abstract boolean tryDeleteItemFromCache(String key);

  /**
   * Handle expected exceptions. Timeout exceptions can occur in a couple of ways when attempting
   * to get something from the cache. To keep the error handling cleaner, this code has been
   * refactored into a dedicated method
   *
   * @param key The cache key used 
   * @param startTime The start time of cache call 
   * @param exception The exception thrown 
   * @param expectedCacheTimeoutSecs The number of seconds after which a timeout is expected.
   * @param action the action attempted when exception was thrown
   */
  protected void handleException(String key, long startTime, Exception exception,
      int expectedCacheTimeoutSecs, String action) {
    long elapsedSecs = unixTimeSupplier.get() - startTime;

    if (exception instanceof InterruptedException) {
      logger.warn("InterruptedException while trying to " + action + " cache item using key "
              + key, exception);
    } else if (classTimeoutException.isInstance(exception) || (exception.getCause() != null
        && classTimeoutException.isInstance(exception.getCause()))) {
      String msg = "Timeout while trying to " + action + " item using key " + key + " after " 
          + elapsedSecs + " seconds.";

      //We sometimes get timeout ex even though we have not reached
      //the configured timeout. As such, we'll only log an error if the timeout has 
      //genuinely been reached
      if (elapsedSecs < expectedCacheTimeoutSecs - 2) {
        logger.warn(msg, exception);
      } else {
        logger.error(msg, exception);

        onCacheTimeout();
      }
    } else {
      logger.error("Failed to " + action + " item using key " + key + " after " + elapsedSecs
          + " seconds.", exception);
    }
  }

  /**
   * Called when a cache operation has timed out
   */
  protected void onCacheTimeout() {
    if (timeoutBucket != null) {
      // Consume one token from the timeout bucket, if there are none left we do not want to 
      // block this thread so use tryConsume here
      timeoutBucket.tryConsume();
    }
  }
  
  /**
   * Get the current active number of connections
   *
   * @return num active connections
   */
  public abstract int getNumActiveConnections();
}
