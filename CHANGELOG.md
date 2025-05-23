# ebx-cachebase-sdk Changelog

## 2.0.2 (Mar 31, 2025)
* Bump com.google.code.gson to `2.10.1` to fix security vulnerability.

## 2.0.1 (Mar 31, 2025)

* Bump lettuce-core dependency to `6.5.5.RELEASE`.

## 2.0.0 (Oct 18, 2024)
* Added support for retrieving items from the cache with their time to live atomically
* This involved adding a new abstract method `getRawCachedItemWithTtl` to `CacheService`
* Note that attempting to retrieve items from the cache with their time to live will always 
  throw an `UnsupportedOperationException` for the `MemcachedCacheService`

## 1.4.1 (Jan 9, 2024)

* Bump lettuce-core dependency to `6.3.0.RELEASE` for redis 7 support

## 1.4.0 (Mar 30, 2023)

* Ensure that the Redis cluster topology is always refreshed and allow the period to be configured
  
## 1.3.0 (Mar 21, 2022)

* Upgrade the dependency `biz.paluch.redis` `lettuce`  to use `io.lettuce` `lettuce-core` which 
  supports Redis 6.x engine as well as any previous Redis engines. The dependency was rewritten 
  so there are no behavioral or breaking changes expected.

## 1.2.1 (Aug 10, 2021)

* Added `CacheWithSupplierFailover`.

## 1.1.1 (Feb 11, 2021)

* Initial release
