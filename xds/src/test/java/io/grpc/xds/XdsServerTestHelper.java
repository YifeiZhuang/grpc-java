/*
 * Copyright 2020 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.xds;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.InsecureChannelCredentials;
import io.grpc.internal.ObjectPool;
import io.grpc.xds.Bootstrapper.BootstrapInfo;
import io.grpc.xds.EnvoyServerProtoData.FilterChain;
import io.grpc.xds.EnvoyServerProtoData.Listener;
import io.grpc.xds.Filter.FilterConfig;
import io.grpc.xds.Filter.NamedFilterConfig;
import io.grpc.xds.VirtualHost.Route;
import io.grpc.xds.XdsClient.LdsUpdate;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;

/**
 * Helper methods related to {@link XdsServerBuilder} and related classes.
 */
public class XdsServerTestHelper {
  private static final Logger log = Logger.getLogger(XdsServerTestHelper.class.getName());

  private static final String SERVER_URI = "trafficdirector.googleapis.com";
  private static final String NODE_ID =
      "projects/42/networks/default/nodes/5c85b298-6f5b-4722-b74a-f7d1f0ccf5ad";
  private static final EnvoyProtoData.Node BOOTSTRAP_NODE =
      EnvoyProtoData.Node.newBuilder().setId(NODE_ID).build();
  static final Bootstrapper.BootstrapInfo BOOTSTRAP_INFO =
      new Bootstrapper.BootstrapInfo(
          Arrays.asList(
              new Bootstrapper.ServerInfo(SERVER_URI, InsecureChannelCredentials.create(), true)),
          BOOTSTRAP_NODE,
          null,
          "grpc/server?udpa.resource.listening_address=%s");

  /** Create an XdsClientWrapperForServerSds with a mock XdsClient. */
  public static XdsClientWrapperForServerSds createXdsClientWrapperForServerSds(int port,
      TlsContextManager tlsContextManager) {
    FakeXdsClientPoolFactory fakeXdsClientPoolFactory = new FakeXdsClientPoolFactory(
        buildMockXdsClient(tlsContextManager));
    return new XdsClientWrapperForServerSds(port, fakeXdsClientPoolFactory);
  }

  private static XdsClient buildMockXdsClient(TlsContextManager tlsContextManager) {
    XdsClient xdsClient = mock(XdsClient.class);
    when(xdsClient.getBootstrapInfo()).thenReturn(BOOTSTRAP_INFO);
    when(xdsClient.getTlsContextManager()).thenReturn(tlsContextManager);
    return xdsClient;
  }

  static XdsClient.LdsResourceWatcher startAndGetWatcher(
      XdsClientWrapperForServerSds xdsClientWrapperForServerSds) {
    xdsClientWrapperForServerSds.start();
    XdsClient mockXdsClient = xdsClientWrapperForServerSds.getXdsClient();
    ArgumentCaptor<XdsClient.LdsResourceWatcher> listenerWatcherCaptor =
        ArgumentCaptor.forClass(null);
    verify(mockXdsClient).watchLdsResource(any(String.class), listenerWatcherCaptor.capture());
    return listenerWatcherCaptor.getValue();
  }

  /**
   * Creates a {@link XdsClient.LdsUpdate} with {@link
   * io.grpc.xds.EnvoyServerProtoData.FilterChain} with a destination port and an optional {@link
   * EnvoyServerProtoData.DownstreamTlsContext}.
   * @param registeredWatcher the watcher on which to generate the update
   * @param tlsContext if non-null, used to populate filterChain
   */
  static void generateListenerUpdate(
      XdsClient.LdsResourceWatcher registeredWatcher,
      EnvoyServerProtoData.DownstreamTlsContext tlsContext, TlsContextManager tlsContextManager) {
    EnvoyServerProtoData.Listener listener = buildTestListener("listener1", "10.1.2.3",
        Arrays.<Integer>asList(), tlsContext, null, tlsContextManager);
    LdsUpdate listenerUpdate = LdsUpdate.forTcpListener(listener);
    registeredWatcher.onChanged(listenerUpdate);
  }

  static void generateListenerUpdate(FakeXdsClient xdsClient,
                                     EnvoyServerProtoData.DownstreamTlsContext tlsContext,
                                     TlsContextManager tlsContextManager) {
    EnvoyServerProtoData.Listener listener = buildTestListener("listener1", "10.1.2.3",
            Arrays.<Integer>asList(), tlsContext, null, tlsContextManager);
    LdsUpdate listenerUpdate = LdsUpdate.forTcpListener(listener);
    xdsClient.deliverLdsUpdate(listenerUpdate);
  }

  static void generateListenerUpdate(
      XdsClient.LdsResourceWatcher registeredWatcher, List<Integer> sourcePorts,
      EnvoyServerProtoData.DownstreamTlsContext tlsContext,
      EnvoyServerProtoData.DownstreamTlsContext tlsContextForDefaultFilterChain,
      TlsContextManager tlsContextManager) {
    EnvoyServerProtoData.Listener listener = buildTestListener("listener1", "10.1.2.3", sourcePorts,
        tlsContext, tlsContextForDefaultFilterChain, tlsContextManager);
    LdsUpdate listenerUpdate = LdsUpdate.forTcpListener(listener);
    registeredWatcher.onChanged(listenerUpdate);
  }

  static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  static EnvoyServerProtoData.Listener buildTestListener(
      String name, String address, List<Integer> sourcePorts,
      EnvoyServerProtoData.DownstreamTlsContext tlsContext,
      EnvoyServerProtoData.DownstreamTlsContext tlsContextForDefaultFilterChain,
      TlsContextManager tlsContextManager) {
    EnvoyServerProtoData.FilterChainMatch filterChainMatch1 =
        new EnvoyServerProtoData.FilterChainMatch(
            0,
            Arrays.<EnvoyServerProtoData.CidrRange>asList(),
            Arrays.<String>asList(),
            Arrays.<EnvoyServerProtoData.CidrRange>asList(),
            null,
            sourcePorts,
            Arrays.<String>asList(),
            null);
    VirtualHost virtualHost =
            VirtualHost.create(
                    "virtual-host", Collections.singletonList("auth"), new ArrayList<Route>(),
                    ImmutableMap.<String, FilterConfig>of());
    HttpConnectionManager httpConnectionManager = HttpConnectionManager.forVirtualHosts(
            0L, Collections.singletonList(virtualHost), new ArrayList<NamedFilterConfig>());
    EnvoyServerProtoData.FilterChain filterChain1 = new EnvoyServerProtoData.FilterChain(
        "filter-chain-foo", filterChainMatch1, httpConnectionManager, tlsContext,
        tlsContextManager);
    EnvoyServerProtoData.FilterChain defaultFilterChain = new EnvoyServerProtoData.FilterChain(
        "filter-chain-bar", null, httpConnectionManager, tlsContextForDefaultFilterChain,
        tlsContextManager);
    EnvoyServerProtoData.Listener listener =
        new EnvoyServerProtoData.Listener(
            name, address, Arrays.asList(filterChain1), defaultFilterChain);
    return listener;
  }

  static final class FakeXdsClientPoolFactory
        implements XdsNameResolverProvider.XdsClientPoolFactory {

    private XdsClient xdsClient;

    FakeXdsClientPoolFactory(XdsClient xdsClient) {
      this.xdsClient = xdsClient;
    }

    @Override
    public void setBootstrapOverride(Map<String, ?> bootstrap) {
      throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    @Nullable
    public ObjectPool<XdsClient> get() {
      throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public ObjectPool<XdsClient> getOrCreate() throws XdsInitializationException {
      return new ObjectPool<XdsClient>() {
        @Override
        public XdsClient getObject() {
          return xdsClient;
        }

        @Override
        public XdsClient returnObject(Object object) {
          xdsClient.shutdown();
          return null;
        }
      };
    }
  }

  static final class FakeXdsClient extends XdsClient {
    boolean shutdown;
    SettableFuture<String> ldsResource = SettableFuture.create();
    LdsResourceWatcher ldsWatcher;
    private final Map<String, RdsResourceWatcher> rdsResources = new HashMap<>();

    @Override
    public TlsContextManager getTlsContextManager() {
      return null;
    }

    @Override
    public BootstrapInfo getBootstrapInfo() {
      return BOOTSTRAP_INFO;
    }

    @Override
    void watchLdsResource(String resourceName, LdsResourceWatcher watcher) {
      assertThat(ldsWatcher).isNull();
      ldsWatcher = watcher;
      ldsResource.set(resourceName);
    }

    @Override
    void cancelLdsResourceWatch(String resourceName, LdsResourceWatcher watcher) {
      assertThat(ldsWatcher).isNotNull();
      ldsResource = null;
      ldsWatcher = null;
    }

    @Override
    synchronized void watchRdsResource(String resourceName, RdsResourceWatcher watcher) {
      rdsResources.put(resourceName, watcher);
    }

    @Override
    synchronized void cancelRdsResourceWatch(String resourceName, RdsResourceWatcher watcher) {
      rdsResources.remove(resourceName);
    }

    @Override
    void shutdown() {
      shutdown = true;
    }

    @Override
    boolean isShutDown() {
      return shutdown;
    }

    void deliverLdsUpdate(List<FilterChain> filterChains,
                                       FilterChain defaultFilterChain) {
      ldsWatcher.onChanged(LdsUpdate.forTcpListener(new Listener(
              "listener", "0.0.0.0:1", filterChains, defaultFilterChain)));
    }

    void deliverLdsUpdate(LdsUpdate ldsUpdate) {
      ldsWatcher.onChanged(ldsUpdate);
    }

    void deliverRdsUpdate(String rdsName, List<VirtualHost> virtualHosts) {
      log.log(Level.INFO, "delivering rds update, current resources: " + rdsResources );
      rdsResources.get(rdsName).onChanged(new RdsUpdate(virtualHosts));
    }

    synchronized Map<String, RdsResourceWatcher> getRdsResources() {
      return ImmutableMap.copyOf(rdsResources);
    }
  }
}
