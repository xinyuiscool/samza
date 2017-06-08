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

package org.apache.samza.control;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.control.ControlMessageAggregator.ControlMessageManager;
import org.apache.samza.message.EndOfStreamMessage;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.util.IOGraphUtil.IONode;
import org.apache.samza.system.EndOfStream;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;


public class EndOfStreamManager implements ControlMessageManager {
  private static final String EOS_KEY_FORMAT = "%s-%s-EOS"; //stream-task-EOS

  private final String taskName;
  private final int taskCount;
  private final MessageCollector collector;
  private final Map<SystemStreamPartition, EndOfStreamState> inputStates;
  private final Map<String, SystemAdmin> sysAdmins;

  public EndOfStreamManager(String taskName,
      int taskCount,
      Set<SystemStreamPartition> ssps,
      Map<String, SystemAdmin> sysAdmins, MessageCollector collector) {
    this.taskName = taskName;
    this.taskCount = taskCount;
    this.sysAdmins = sysAdmins;
    this.collector = collector;
    Map<SystemStreamPartition, EndOfStreamState> states = new HashMap<>();
    ssps.forEach(ssp -> {
        states.put(ssp, new EndOfStreamState());
      });
    this.inputStates = Collections.unmodifiableMap(states);
  }

  @Override
  public IncomingMessageEnvelope update(IncomingMessageEnvelope envelope) {
    EndOfStreamState state = inputStates.get(envelope.getSystemStreamPartition());
    EndOfStreamMessage message = (EndOfStreamMessage) envelope.getMessage();
    state.update(message.getTaskName(), message.getTaskCount());

    // if all the partitions for this system stream is end-of-stream, we generate
    // EndOfStream for the streamId
    SystemStreamPartition ssp = envelope.getSystemStreamPartition();
    SystemStream systemStream = ssp.getSystemStream();
    if (isEndOfStream(systemStream)) {
      EndOfStream eos = new EndOfStreamImpl(ssp, this);
      return new IncomingMessageEnvelope(ssp, IncomingMessageEnvelope.END_OF_STREAM_OFFSET, envelope.getKey(), eos);
    } else {
      return null;
    }
  }

  public boolean isEndOfStream(SystemStream systemStream) {
    return inputStates.entrySet().stream()
        .filter(entry -> entry.getKey().getSystemStream().equals(systemStream))
        .allMatch(entry -> entry.getValue().isEndOfStream());
  }

  public void sendEndOfStream(SystemStream systemStream) {
    String stream = systemStream.getStream();
    SystemStreamMetadata metadata = sysAdmins.get(systemStream.getSystem())
        .getSystemStreamMetadata(Collections.singleton(stream))
        .get(stream);
    int partitionCount = metadata.getSystemStreamPartitionMetadata().size();
    for (int i = 0; i < partitionCount; i++) {
      String key = String.format(EOS_KEY_FORMAT, stream, taskName);
      EndOfStreamMessage message = new EndOfStreamMessage(taskName, taskCount);
      OutgoingMessageEnvelope envelopeOut = new OutgoingMessageEnvelope(systemStream, i, key, message);
      collector.send(envelopeOut);
    }
  }

  private static final class EndOfStreamImpl implements EndOfStream {
    private final SystemStreamPartition ssp;
    private final EndOfStreamManager manager;

    private EndOfStreamImpl(SystemStreamPartition ssp, EndOfStreamManager manager) {
      this.ssp = ssp;
      this.manager = manager;
    }

    @Override
    public SystemStreamPartition get() {
      return ssp;
    }

    EndOfStreamManager getManager() {
      return manager;
    }
  }

  private final static class EndOfStreamState {
    private Set<String> tasks;
    private int expectedTotal = Integer.MAX_VALUE;
    private boolean isEndOfStream = false;

    void update(String taskName, int taskCount) {
      tasks.add(taskName);
      expectedTotal = taskCount;
      isEndOfStream = tasks.size() == expectedTotal;
    }

    boolean isEndOfStream() {
      return isEndOfStream;
    }
  }

  public static final class EndOfStreamDispatcher {
    private final Multimap<SystemStream, IONode> ioGraph = HashMultimap.create();

    private EndOfStreamDispatcher(StreamGraphImpl streamGraph) {
      streamGraph.toIOGraph().forEach(node -> {
          node.getInputs().forEach(stream -> {
              ioGraph.put(new SystemStream(stream.getSystemName(), stream.getPhysicalName()), node);
            });
        });
    }

    public void sendToDownstream(EndOfStream endOfStream) {
      EndOfStreamManager manager = ((EndOfStreamImpl) endOfStream).getManager();
      ioGraph.get(endOfStream.get()).forEach(node -> {
          if (node.getOutputOpSpec().getOpCode() == OperatorSpec.OpCode.PARTITION_BY) {
            boolean isEndOfStream =
                node.getInputs().stream().allMatch(spec -> manager.isEndOfStream(spec.toSystemStream()));
            if (isEndOfStream) {
              // broadcast the end-of-stream message to the intermediate stream
              manager.sendEndOfStream(node.getOutput().toSystemStream());
            }
          }
        });
    }
  }

  /**
   * Builds an end-of-stream envelope for an SSP.
   *
   * @param ssp The SSP that is at end-of-stream.
   * @return an IncomingMessageEnvelope corresponding to end-of-stream for that SSP.
   */
  public static IncomingMessageEnvelope buildEndOfStreamEnvelope(SystemStreamPartition ssp) {
    return new IncomingMessageEnvelope(ssp, IncomingMessageEnvelope.END_OF_STREAM_OFFSET, null, new EndOfStreamMessage(null, 0));
  }

  public static EndOfStreamDispatcher createDispatcher(StreamGraphImpl streamGraph) {
    return new EndOfStreamDispatcher(streamGraph);
  }
}
