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
package org.apache.reef.examples.suspend;

import com.sun.tools.javac.util.List;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.reef.examples.suspend.Launch.runSuspendReef;

/**
 * Suspend/Resume example - main class.
 */
public final class LaunchYarn {

  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(LaunchYarn.class.getName());

  /**
   * This class should not be instantiated.
   */
  private LaunchYarn() {
    throw new RuntimeException("Do not instantiate this class!");
  }

  /**
   * Main method that runs the example.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) {
    try {
      final Configuration runtimeConfiguration = YarnClientConfiguration.CONF.build();
      runSuspendReef(runtimeConfiguration, (String[])List.from(args).append("-local").append("false").toArray());
    } catch (final BindException | IOException | InjectionException ex) {
      LOG.log(Level.SEVERE, "Cannot launch: configuration error", ex);
    } catch (final Exception ex) {
      LOG.log(Level.SEVERE, "Cleanup error", ex);
    }
  }
}
