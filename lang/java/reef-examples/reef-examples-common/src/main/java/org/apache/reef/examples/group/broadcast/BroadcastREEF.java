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
package org.apache.reef.examples.group.broadcast;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.examples.group.bgd.parameters.ModelDimensions;
import org.apache.reef.examples.group.broadcast.parameters.NumberOfReceivers;
import org.apache.reef.io.network.group.impl.driver.GroupCommService;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

@ClientSide
public final class BroadcastREEF {
  private static final Logger LOG = Logger.getLogger(BroadcastREEF.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 2 * 60 * 1000;

  private static int dimensions;
  private static int numberOfReceivers;

  private static Configuration parseCommandLine(final String[] aArgs) {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    try {
      final CommandLine cl = new CommandLine(cb);
      cl.registerShortNameOfClass(ModelDimensions.class);
      cl.registerShortNameOfClass(NumberOfReceivers.class);
      cl.processCommandLine(aArgs);
    } catch (final IOException ex) {
      final String msg = "Unable to parse command line";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
    return cb.build();
  }

  /**
   * copy the parameters from the command line required for the Client configuration.
   */
  private static void storeCommandLineArgs(
      final Configuration commandLineConf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    dimensions = injector.getNamedInstance(ModelDimensions.class);
    numberOfReceivers = injector.getNamedInstance(NumberOfReceivers.class);
  }
  public static LauncherStatus runBGDReef(
      final Configuration runtimeConfiguration, final String[] args) throws InjectionException {
    final Configuration commandLineConf = parseCommandLine(args);
    storeCommandLineArgs(commandLineConf);

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .setMultiple(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getAllClasspathJars())
        .set(DriverConfiguration.ON_DRIVER_STARTED, BroadcastDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, BroadcastDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, BroadcastDriver.ContextActiveHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, BroadcastDriver.ContextCloseHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, BroadcastDriver.FailedTaskHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "BroadcastDriver")
        .build();

    final Configuration groupCommServConfiguration = GroupCommService.getConfiguration();

    final Configuration mergedDriverConfiguration = Tang.Factory.getTang()
        .newConfigurationBuilder(groupCommServConfiguration, driverConfiguration)
        .bindNamedParameter(ModelDimensions.class, Integer.toString(dimensions))
        .bindNamedParameter(NumberOfReceivers.class, Integer.toString(numberOfReceivers))
        .build();

    LOG.info(new AvroConfigurationSerializer().toString(mergedDriverConfiguration));

    return DriverLauncher.getLauncher(runtimeConfiguration).run(mergedDriverConfiguration, JOB_TIMEOUT);
  }

    /**
     * Empty private constructor to prohibit instantiation of utility class.
     */
  private BroadcastREEF() {
  }
}
