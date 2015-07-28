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
package org.apache.reef.examples.data.output;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

/**
 * Client for the output service demo app.
 */
@ClientSide
public final class OutputServiceREEF {
  /**
   * @return The Driver configuration.
   */
  private static Configuration getDriverConf() {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(OutputServiceDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "OutputServiceREEF")
        .set(DriverConfiguration.ON_DRIVER_STARTED, OutputServiceDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, OutputServiceDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, OutputServiceDriver.ActiveContextHandler.class)
        .build();

    return driverConf;
  }

  public static LauncherStatus runOutputServiceReef(final Configuration runtimeConf,
                                                    final Configuration outputServiceConf, final int timeOut)
      throws BindException, InjectionException {
    final Configuration driverConf = getDriverConf();
    final Configuration submittedConfiguration = Tang.Factory.getTang()
        .newConfigurationBuilder(driverConf, outputServiceConf)
        .build();
    return DriverLauncher.getLauncher(runtimeConf).run(submittedConfiguration, timeOut);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private OutputServiceREEF() {
  }
}
