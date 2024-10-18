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

/**
 * Represents the result of retrieving from the cache
 * @param <T> - the type of the value
 * @author KHassan98
 */
public class CachedResult<T> {
  private final T value;
  private final Long ttl;
  
  public CachedResult(T value, Long ttl) {
    this.value = value;
    this.ttl = ttl;
  }
  
  public T getValue() {
    return value;
  }
  
  public Long getTtl() {
    return ttl;
  }
}