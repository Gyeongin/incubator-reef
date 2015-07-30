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
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.util.EnvironmentUtils;

import java.io.IOException;

/**
 * Client for the data loading demo app.
 */
@ClientSide
public final class DataLoadingREEF {

  private static final int NUM_SPLITS = 6;
  private static final int NUM_COMPUTE_EVALUATORS = 2;

  private static Configuration parseCommandLine(final String[] args) throws IOException{
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    new CommandLine(cb)
        .registerShortNameOfClass(TimeOut.class)
        .registerShortNameOfClass(InputDir.class)
        .processCommandLine(args);

    return cb.build();
  }

  private static ConfigurationModule getDriverConfigurationModule() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(LineCounter.class))
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, LineCounter.ContextActiveHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, LineCounter.TaskCompletedHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "DataLoadingREEF");
  }

  private static Configuration getDataLoadConfiguration(final String inputDir) {
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

  public static LauncherStatus runDataLoadingReef(final Configuration runtimeConf, final String[] args)
      throws BindException, InjectionException, IOException {
    final Configuration commandLineConf = parseCommandLine(args);
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final int timeout = injector.getNamedInstance(TimeOut.class) * 60 * 1000;
    final String inputDir = injector.getNamedInstance(InputDir.class);

    final Configuration dataLoadConf = getDataLoadConfiguration(inputDir);
    return DriverLauncher.getLauncher(runtimeConf).run(dataLoadConf, timeout);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private DataLoadingREEF() {
  }

  @NamedParameter(doc = "Number of minutes before timeout",
      short_name = "timeout", default_value = "2")
  public static final class TimeOut implements Name<Integer> {
  }

  @NamedParameter(short_name = "input")
  public static final class InputDir implements Name<String> {
  }
}
