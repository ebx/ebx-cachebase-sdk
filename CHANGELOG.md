# ebx-cachebase-sdk Changelog

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
