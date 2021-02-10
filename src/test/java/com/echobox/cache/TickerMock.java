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

import com.google.common.base.Ticker;
import mockit.Mock;
import mockit.MockUp;

/**
 * Ticker implementation to mock time
 *
 * @author eddspencer
 */
public class TickerMock extends MockUp<Ticker> {
  private long time = 0;

  /**
   * Sets time.
   *
   * @param time the time
   */
  public void setTime(final long time) {
    this.time = time;
  }

  /**
   * Read time.
   *
   * @return the time
   */
  @Mock
  public long read() {
    return time;
  }

  /**
   * Mock the system ticker.
   *
   * @return the ticker
   */
  @Mock
  public Ticker systemTicker() {
    return new Ticker() {
      @Override
      public long read() {
        return time;
      }
    };
  }
}
