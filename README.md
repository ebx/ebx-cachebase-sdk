[![Maven Central](https://img.shields.io/maven-central/v/com.echobox/ebx-cachebase-sdk.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.echobox%22%20AND%20a:%22ebx-cachebase-sdk%22) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://raw.githubusercontent.com/ebx/ebx-cachebase-sdk/master/LICENSE) [![Build Status](https://travis-ci.org/ebx/ebx-cachebase-sdk.svg?branch=dev)](https://travis-ci.org/ebx/ebx-cachebase-sdk)
# ebx-cachebase-sdk

Caching is a common component in almost every production application. This SDK provides a
consistent interface, `CacheService`, and implementations that decorate different
underlying implementations (`NoOpCacheService`, `InMemoryCacheService`, `MemcachedCacheService
`, `RedisCacheService` and `RedisCacheServiceAsBytes`).

This allows caching to be implemented in the most common use cases concisely:

```
RedisCacheService.initialise(clusterEndPointIP, clusterPort, shutdownMonitor);
CachService service = RedisCacheService.getInstance();

service.trySaveItemToCache(...
// and
service.tryGetCachedItem(...
```

Using `CacheService` we also provide a `CacheCheckPeriodic` utility that allows a cached value to
be checked periodically, using an in memory result otherwise. This allows for high performance
[feature-flags](https://en.wikipedia.org/wiki/Feature_toggle).

## Installation

For our latest stable release use:

```
<dependency>
  <groupId>com.echobox</groupId>
  <artifactId>ebx-cachebase-sdk</artifactId>
  <version>1.1.0</version>
</dependency>