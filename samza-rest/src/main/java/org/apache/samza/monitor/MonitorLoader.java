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
package org.apache.samza.monitor;

import org.apache.samza.metrics.MetricsRegistry;


public class MonitorLoader {

  /**
   *
   * @param monitorConfig contains the configuration defined for a particular monitor.
   * @param metricsRegistry instance that will be used to register custom metrics.
   * @return the instantiated monitor object.
   * @throws InstantiationException when there is a exception instantiating the monitor.
   */
  public static Monitor instantiateMonitor(MonitorConfig monitorConfig, MetricsRegistry metricsRegistry)
      throws InstantiationException {
      String factoryClass = monitorConfig.getMonitorFactoryClass();
      try {
        MonitorFactory monitorFactory = (MonitorFactory) Class.forName(factoryClass).newInstance();
        return monitorFactory.getMonitorInstance(monitorConfig, metricsRegistry);
      } catch (Exception e) {
        throw (InstantiationException)
            new InstantiationException("Unable to instantiate monitor with factory class " + factoryClass).initCause(e);
      }
  }
}
