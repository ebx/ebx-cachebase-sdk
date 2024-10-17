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
  <version>2.0.0</version>
</dependency>
```

## Getting in touch

* **[GitHub Issues](https://github.com/ebx/ebx-cachebase-sdk/issues/new)**: If you have ideas, bugs, 
or problems with our library, just open a new issue.

## Contributing

If you would like to get involved please follow the instructions 
[here](https://github.com/ebx/ebx-cachebase-sdk/tree/master/CONTRIBUTING.md)

## Releases

We use [semantic versioning](https://semver.org/).

All merges into DEV will automatically get released as a maven central snapshot, which can be easily
included in any downstream dependencies that always desire the latest changes (see above for 
'Most Up To Date' installation).

Each merge into the MASTER branch will automatically get released to Maven central and github 
releases, using the current library version. As such, following every merge to master, the version 
number of the dev branch should be incremented and will represent 'Work In Progress' towards the 
next release. 

Please use a merge (not rebase) commit when merging dev into master to perform the release.

To create a full release to Maven central please follow these steps:
1. Ensure the `CHANGELOG.md` is up to date with all the changes in the release, if not please raise 
a suitable PR into `DEV`. Typically the change log should be updated as we go.
3. Create a PR from `DEV` into `MASTER`. Ensure the version in the `pom.xml` is the 
correct version to be released. Merging this PR into `MASTER` will automatically create the maven 
and github releases. Please note that a release is final, it can not be undone/deleted/overwritten.
5. Once the public release has been successful create a final PR into `DEV` that contains an 
incremented `pom.xml` version to ensure the correct snapshot gets updated on subsequent merges
into `DEV`. This PR should also include:
    * An update to the `README.md` latest stable release version number.
    * A 'Work In Progress' entry for the next anticipated release in `CHANGELOG.md`.