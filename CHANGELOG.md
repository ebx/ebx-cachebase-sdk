# ebx-cachebase-sdk Changelog

## 1.2.1 (Aug 10, 2021)

* Added `CacheWithSupplierFailover`.
  The cache that periodically gets the latest value from the provided source of truth.
  If source of truth is not available then it will use
  cached value for a specified period of time before throwing an error.

## 1.1.1 (Feb 11, 2021)

* Initial release
