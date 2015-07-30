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
package org.apache.reef.examples.pool;

import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.reef.examples.pool.Launch.runPoolReef;

/**
 * Pool of Evaluators example - main class.
 */
public final class LaunchLocal {

  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  private static final int MAX_NUMBER_OF_EVALUATORS = 4;
  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(LaunchLocal.class.getName());

  /**
   * This class should not be instantiated.
   */
  private LaunchLocal() {
    throw new RuntimeException("Do not instantiate this class!");
  }

  /**
   * Main method that launches the REEF job.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) {
    try {
      LOG.log(Level.FINE, "Running on the local runtime");
      final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .build();

      runPoolReef(runtimeConfiguration, args);

    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }
}
