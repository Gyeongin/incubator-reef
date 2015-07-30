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
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.data.output.TaskOutputStreamProviderLocal;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.reef.examples.data.output.OutputServiceREEF.runOutputServiceReef;

/**
 * Client for the output service demo app.
 */
@ClientSide
public final class OutputServiceREEFLocal {
  private static final Logger LOG = Logger.getLogger(OutputServiceREEFLocal.class.getName());

  public static void main(final String[] args) throws InjectionException, BindException, IOException {

    LOG.log(Level.INFO, "Running the output service demo on the local runtime");
    final Configuration runtimeConf = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, 3)
        .build();
    final LauncherStatus state = runOutputServiceReef(runtimeConf, TaskOutputStreamProviderLocal.class, args, true);

    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private OutputServiceREEFLocal() {
  }
}
