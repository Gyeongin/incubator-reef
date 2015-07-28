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

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.reef.examples.data.loading.DataLoadingREEF.runDataLoadingReef;

/**
 * Client for the data loading demo app.
 */
@ClientSide
public final class DataLoadingREEFYarn {

  private static final Logger LOG = Logger.getLogger(DataLoadingREEFYarn.class.getName());

  public static void main(final String[] args)
      throws InjectionException, BindException, IOException {

    final Tang tang = Tang.Factory.getTang();

    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    new CommandLine(cb)
        .registerShortNameOfClass(TimeOut.class)
        .registerShortNameOfClass(InputDir.class)
        .processCommandLine(args);

    final Injector injector = tang.newInjector(cb.build());

    final int jobTimeout = injector.getNamedInstance(TimeOut.class) * 60 * 1000;
    final String inputDir = injector.getNamedInstance(InputDir.class);

    LOG.log(Level.INFO, "Running Data Loading demo on YARN");
    final Configuration runtimeConfiguration = YarnClientConfiguration.CONF.build();

    final LauncherStatus state = runDataLoadingReef(runtimeConfiguration, inputDir, jobTimeout);

    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }

  @NamedParameter(doc = "Number of minutes before timeout",
      short_name = "timeout", default_value = "2")
  public static final class TimeOut implements Name<Integer> {
  }

  @NamedParameter(short_name = "input")
  public static final class InputDir implements Name<String> {
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private DataLoadingREEFYarn() {
  }
}
