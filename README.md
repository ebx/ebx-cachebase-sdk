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