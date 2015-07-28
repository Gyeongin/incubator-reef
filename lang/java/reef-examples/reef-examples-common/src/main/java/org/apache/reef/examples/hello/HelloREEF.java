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

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.client.REEF;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

/**
 * The Client for Hello REEF example.
 */
public final class HelloREEF {

  /**
   * @return the configuration of the HelloREEF driver.
   */
  private static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(HelloDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
        .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
        .build();
  }

  public static LauncherStatus runHelloReef(final Configuration runtimeConf)
      throws BindException, InjectionException {
    final Configuration driverConf = getDriverConfiguration();
    return DriverLauncher.getLauncher(runtimeConf).run(driverConf);
  }

  public static LauncherStatus runHelloReef(final Configuration runtimeConf, final int timeOut)
      throws BindException, InjectionException {
    final Configuration driverConf = getDriverConfiguration();
    return DriverLauncher.getLauncher(runtimeConf).run(driverConf, timeOut);
  }

  public static void runHelloReefWithoutClient(final Configuration runtimeConf) throws InjectionException {
    final REEF reef = Tang.Factory.getTang().newInjector(runtimeConf).getInstance(REEF.class);
    final Configuration driverConf = getDriverConfiguration();

    reef.submit(driverConf);
  }

    /**
     * Empty private constructor to prohibit instantiation of utility class.
     */
  private HelloREEF() {
  }
}
