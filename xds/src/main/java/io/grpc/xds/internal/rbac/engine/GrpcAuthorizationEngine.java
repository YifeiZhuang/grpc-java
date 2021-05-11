/*
 * Copyright 2021 The gRPC Authors
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

package io.grpc.xds.internal.rbac.engine;

import io.envoyproxy.envoy.config.rbac.v3.Policy;
import io.envoyproxy.envoy.config.rbac.v3.RBAC;
import io.envoyproxy.envoy.config.rbac.v3.RBAC.Action;
import io.grpc.xds.EvaluateArgs;
import io.grpc.xds.internal.rbac.engine.AuthorizationEngineInterface.AuthDecision.DecisionType;
import java.util.HashMap;
import java.util.Map;

/** Implementation of gRPC server access control based on envoy RBAC protocol:
 * https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/rbac/v3/rbac.proto
 * One GrpcAuthorizationEngine corresponds to one action type and a list of policies.
 * Action type is either DENY or ALLOW. Policies are examined sequentially in an any match fashion.
 * If not matched, the opposite action type is returned as a result.
 * e.g.
 * action: ALLOW
 * policies:
 *   "service-admin":
 *     permissions:
 *       - any: true
 *     principals:
 *       - authenticated:
 *           principal_name:
 *             exact: "cluster.local/ns/default/sa/admin"
 *       - authenticated:
 *           principal_name:
 *             exact: "cluster.local/ns/default/sa/superuser"
 *   "product-viewer":
 *     permissions:
 *         - and_rules:
 *             rules:
 *               - header: { name: ":method", exact_match: "GET" }
 *               - url_path:
 *                   path: { prefix: "/products" }
 *               - or_rules:
 *                   rules:
 *                     - destination_port: 80
 *                     - destination_port: 443
 *     principals:
 *       - any: true
 * */
public class GrpcAuthorizationEngine implements AuthorizationEngineInterface {
  private final Map<String, PolicyMatcher> policyMatchers = new HashMap<>();
  private final Action action;

  /** Instantiate with envoy RBAC policy configuration. */
  public GrpcAuthorizationEngine(RBAC rbacPolicy) {
    this.action = rbacPolicy.getAction();
    for (Map.Entry<String, Policy> entry: rbacPolicy.getPoliciesMap().entrySet()) {
      policyMatchers.put(entry.getKey(), new PolicyMatcher(entry.getValue()));
    }
  }

  /** The matching criteria are supposed to be immutable once constructed,
   * and {@link #evaluate(EvaluateArgs)} are supposed to be called multiple times safely. */
  @Override
  public AuthDecision evaluate(EvaluateArgs args) {
    String firstMatch = findFirstMatch(args);
    DecisionType decisionType = DecisionType.DENY;
    if (Action.DENY.equals(action) == (firstMatch == null)) {
      decisionType = DecisionType.ALLOW;
    }
    return AuthDecision.create(decisionType, firstMatch);
  }

  /** Sequentially evaluate each policy, and return the policy name for the first match if any
   * match happens, otherwise return null. */
  private String findFirstMatch(EvaluateArgs args) {
    for (Map.Entry<String, PolicyMatcher> entry: policyMatchers.entrySet()) {
      if (entry.getValue().matches(args)) {
        return entry.getKey();
      }
    }
    return null;
  }
}
