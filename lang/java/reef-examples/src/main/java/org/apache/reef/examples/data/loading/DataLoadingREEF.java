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
package org.apache.reef.examples.data.loading;

import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.io.data.loading.api.DataLoadingRequestBuilder;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.util.EnvironmentUtils;

/**
 * Client for the data loading demo app.
 */
@ClientSide
public final class DataLoadingREEF {

  private static final int NUM_SPLITS = 6;
  private static final int NUM_COMPUTE_EVALUATORS = 2;

  public static ConfigurationModule getDriverConfigurationModule() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(LineCounter.class))
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, LineCounter.ContextActiveHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, LineCounter.TaskCompletedHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "DataLoadingREEF");
  }

  public static Configuration getDataLoadConfiguration(final String inputDir) {
    final EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
        .setNumber(NUM_COMPUTE_EVALUATORS)
        .setMemory(512)
        .setNumberOfCores(1)
        .build();

    return new DataLoadingRequestBuilder()
        .setMemoryMB(1024)
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(inputDir)
        .setNumberOfDesiredSplits(NUM_SPLITS)
        .setComputeRequest(computeRequest)
        .setDriverConfigurationModule(getDriverConfigurationModule())
        .build();
  }

  public static LauncherStatus runDataLoadingReef(final Configuration runtimeConf,
                                                  final String inputDir, final int timeOut)
      throws BindException, InjectionException {
    final Configuration dataLoadConf = getDataLoadConfiguration(inputDir);
    return DriverLauncher.getLauncher(runtimeConf).run(dataLoadConf, timeOut);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private DataLoadingREEF() {
  }
}
