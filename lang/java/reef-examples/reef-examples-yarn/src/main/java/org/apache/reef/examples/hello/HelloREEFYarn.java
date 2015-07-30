/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.examples.hello;

import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.reef.examples.hello.HelloREEF.runHelloReef;

/**
 * The Client for running HelloREEF on YARN.
 */
public final class HelloREEFYarn {

  private static final Logger LOG = Logger.getLogger(HelloREEFYarn.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   * Setting to 100 sec because running on RM HA clusters take around
   * 50 seconds to set the job to running.
   */
  private static final int JOB_TIMEOUT = 100000; // 100 sec.

  /**
   * Start Hello REEF job. Runs method runHelloReef().
   *
   * @param args command line parameters.
   * @throws org.apache.reef.tang.exceptions.BindException      configuration error.
   * @throws org.apache.reef.tang.exceptions.InjectionException configuration error.
   */
  public static void main(final String[] args) throws BindException, InjectionException {
    final Configuration runtimeConfiguration = YarnClientConfiguration.CONF.build();
    final LauncherStatus status = runHelloReef(runtimeConfiguration, JOB_TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private HelloREEFYarn() {
  }
}
