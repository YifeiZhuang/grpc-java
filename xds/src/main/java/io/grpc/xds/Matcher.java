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

package io.grpc.xds;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.re2j.Pattern;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/** Defines matcher abstract and provides a group of request matchers.
 * A matcher implementation evaluates an {@link EvaluateArgs} input and tells whether certain
 * argument matches a predefined match pattern. */
public abstract class Matcher {
  protected Matcher() {}

  public abstract boolean matches(EvaluateArgs args);

  /** Matches when any of the matcher matches. */
  public static class OrMatcher extends Matcher {
    private final List<? extends Matcher> anyMatch;

    public OrMatcher(List<? extends Matcher> matchers) {
      checkNotNull(matchers, "matchers");
      this.anyMatch = Collections.unmodifiableList(matchers);
    }

    @Override
    public boolean matches(EvaluateArgs args) {
      for (Matcher m : anyMatch) {
        if (m.matches(args)) {
          return true;
        }
      }
      return false;
    }
  }

  /** Matches when all of the matchers matches. */
  public static class AndMatcher extends Matcher {
    private final List<? extends Matcher> allMatch;

    public AndMatcher(List<? extends Matcher> matchers) {
      checkNotNull(matchers, "matchers");
      this.allMatch = Collections.unmodifiableList(matchers);
    }

    @Override
    public boolean matches(EvaluateArgs args) {
      for (Matcher m : allMatch) {
        if (!m.matches(args)) {
          return false;
        }
      }
      return true;
    }
  }

  /** Always true matcher.*/
  public static class AlwaysTrueMatcher extends Matcher {
    public static AlwaysTrueMatcher INSTANCE = new AlwaysTrueMatcher();

    @Override
    public boolean matches(EvaluateArgs args) {
      return true;
    }
  }

  /** Negate matcher.*/
  public static class InvertMatcher extends Matcher {
    private final Matcher toInvertMatcher;

    public InvertMatcher(Matcher matcher) {
      this.toInvertMatcher = matcher;
    }

    @Override
    public boolean matches(EvaluateArgs args) {
      return !toInvertMatcher.matches(args);
    }
  }

  /** Matcher for HTTP request path. */
  @AutoValue
  abstract static class RouteMatcher extends Matcher {
    // Exact full path to be matched.
    @Nullable
    abstract String path();

    // Path prefix to be matched.
    @Nullable
    abstract String prefix();

    // Regular expression pattern of the path to be matched.
    @Nullable
    abstract Pattern regEx();

    // Whether case sensitivity is taken into account for matching.
    // Only valid for full path matching or prefix matching.
    abstract boolean caseSensitive();

    static RouteMatcher fromPath(String path, boolean caseSensitive) {
      checkNotNull(path, "path");
      return RouteMatcher.create(path, null, null, caseSensitive);
    }

    static RouteMatcher fromPrefix(String prefix, boolean caseSensitive) {
      checkNotNull(prefix, "prefix");
      return RouteMatcher.create(null, prefix, null, caseSensitive);
    }

    static RouteMatcher fromRegEx(Pattern regEx) {
      checkNotNull(regEx, "regEx");
      return RouteMatcher.create(null, null, regEx, false /* doesn't matter */);
    }

    private static RouteMatcher create(@Nullable String path, @Nullable String prefix,
        @Nullable Pattern regEx, boolean caseSensitive) {
      return new AutoValue_Matcher_RouteMatcher(path, prefix, regEx, caseSensitive);
    }

    @Override
    public boolean matches(EvaluateArgs args) {
      String fullMethodName = args.getFullMethodName();
      if (path() != null) {
        return caseSensitive()
            ? path().equals(fullMethodName)
            : path().equalsIgnoreCase(fullMethodName);
      } else if (prefix() != null) {
        return caseSensitive()
            ? fullMethodName.startsWith(prefix())
            : fullMethodName.toLowerCase().startsWith(prefix().toLowerCase());
      }
      return regEx().matches(fullMethodName);
    }
  }

  /** Matcher for HTTP request headers. */
  @AutoValue
  abstract static class HeaderMatcher extends Matcher {
    // Name of the header to be matched.
    abstract String name();

    // Matches exact header value.
    @Nullable
    abstract String exactValue();

    // Matches header value with the regular expression pattern.
    @Nullable
    abstract Pattern safeRegEx();

    // Matches header value an integer value in the range.
    @Nullable
    abstract Range range();

    // Matches header presence.
    @Nullable
    abstract Boolean present();

    // Matches header value with the prefix.
    @Nullable
    abstract String prefix();

    // Matches header value with the suffix.
    @Nullable
    abstract String suffix();

    // Whether the matching semantics is inverted. E.g., present && !inverted -> !present
    abstract boolean inverted();

    static HeaderMatcher forExactValue(String name, String exactValue, boolean inverted) {
      checkNotNull(name, "name");
      checkNotNull(exactValue, "exactValue");
      return HeaderMatcher.create(name, exactValue, null, null, null, null, null, inverted);
    }

    static HeaderMatcher forSafeRegEx(String name, Pattern safeRegEx, boolean inverted) {
      checkNotNull(name, "name");
      checkNotNull(safeRegEx, "safeRegEx");
      return HeaderMatcher.create(name, null, safeRegEx, null, null, null, null, inverted);
    }

    static HeaderMatcher forRange(String name, Range range, boolean inverted) {
      checkNotNull(name, "name");
      checkNotNull(range, "range");
      return HeaderMatcher.create(name, null, null, range, null, null, null, inverted);
    }

    static HeaderMatcher forPresent(String name, boolean present, boolean inverted) {
      checkNotNull(name, "name");
      return HeaderMatcher.create(name, null, null, null, present, null, null, inverted);
    }

    static HeaderMatcher forPrefix(String name, String prefix, boolean inverted) {
      checkNotNull(name, "name");
      checkNotNull(prefix, "prefix");
      return HeaderMatcher.create(name, null, null, null, null, prefix, null, inverted);
    }

    static HeaderMatcher forSuffix(String name, String suffix, boolean inverted) {
      checkNotNull(name, "name");
      checkNotNull(suffix, "suffix");
      return HeaderMatcher.create(name, null, null, null, null, null, suffix, inverted);
    }

    private static HeaderMatcher create(String name, @Nullable String exactValue,
        @Nullable Pattern safeRegEx, @Nullable Range range,
        @Nullable Boolean present, @Nullable String prefix,
        @Nullable String suffix, boolean inverted) {
      checkNotNull(name, "name");
      return new AutoValue_Matcher_HeaderMatcher(name, exactValue, safeRegEx, range, present,
          prefix, suffix, inverted);
    }

    /** Represents an integer range. */
    @AutoValue
    abstract static class Range {
      abstract long start();

      abstract long end();

      static Range create(long start, long end) {
        return new AutoValue_Matcher_HeaderMatcher_Range(start, end);
      }
    }

    @Override
    public boolean matches(EvaluateArgs args) {
      String value = args.getHeader(name());
      if (present() != null) {
        return (value == null) == present().equals(inverted());
      }
      if (value == null) {
        return false;
      }
      boolean baseMatch;
      if (exactValue() != null) {
        baseMatch = exactValue().equals(value);
      } else if (safeRegEx() != null) {
        baseMatch = safeRegEx().matches(value);
      } else if (range() != null) {
        long numValue;
        try {
          numValue = Long.parseLong(value);
          baseMatch = numValue >= range().start()
              && numValue <= range().end();
        } catch (NumberFormatException ignored) {
          baseMatch = false;
        }
      } else if (prefix() != null) {
        baseMatch = value.startsWith(prefix());
      } else {
        baseMatch = value.endsWith(suffix());
      }
      return baseMatch != inverted();
    }
  }

  /** Represents a fractional value. */
  @AutoValue
  abstract static class FractionMatcher {
    abstract int numerator();

    abstract int denominator();

    static FractionMatcher create(int numerator, int denominator) {
      return new AutoValue_Matcher_FractionMatcher(numerator, denominator);
    }
  }

  /** Defines string match pattern and evaluates matching result for the argument.*/
  @AutoValue
  public abstract static class StringMatcher extends Matcher {
    // The input string exactly matches the specified string exactly.
    @Nullable
    abstract String exact();

    // The input string has this prefix.
    @Nullable
    abstract String prefix();

    // The input string has this suffix.
    @Nullable
    abstract String suffix();

    // The input string matches the regular expression.
    @Nullable
    abstract Pattern regEx();

    // The input string has this substring.
    @Nullable
    abstract String contains();

    // If true, exact/prefix/suffix matching should be case insensitive. No effect on regular
    // expression matching.
    abstract boolean ignoreCase();

    static StringMatcher forExact(String exact, boolean ignoreCase) {
      checkNotNull(exact, "exact");
      return StringMatcher.create(exact, null, null, null, null,
          ignoreCase);
    }

    static StringMatcher forPrefix(String prefix, boolean ignoreCase) {
      checkNotNull(prefix, "prefix");
      return StringMatcher.create(null, prefix, null, null, null,
          ignoreCase);
    }

    static StringMatcher forSuffix(String suffix, boolean ignoreCase) {
      checkNotNull(suffix, "suffix");
      return StringMatcher.create(null, null, suffix, null, null,
          ignoreCase);
    }

    static StringMatcher forSafeRegEx(Pattern regEx, boolean ignoreCase) {
      checkNotNull(regEx, "regEx");
      return StringMatcher.create(null, null, null, regEx, null,
          ignoreCase);
    }

    static StringMatcher forContains(String contains, boolean ignoreCase) {
      checkNotNull(contains, "contains");
      return StringMatcher.create(null, null, null, null, contains,
          ignoreCase);
    }

    @Override
    public boolean matches(EvaluateArgs args) {
      throw new UnsupportedOperationException("error");
    }

    /** Returns match result for one and only one of the match pattern. */
    public boolean matches(String args) {
      if (args == null) {
        return false;
      }
      if (exact() != null) {
        return ignoreCase()
            ? exact().equalsIgnoreCase(args)
            : exact().equals(args);
      } else if (prefix() != null) {
        return ignoreCase()
            ? args.toLowerCase().startsWith(prefix().toLowerCase())
            : args.startsWith(prefix());
      } else if (suffix() != null) {
        return ignoreCase()
            ? args.toLowerCase().endsWith(suffix().toLowerCase())
            : args.endsWith(suffix());
      } else if (contains() != null) {
        return args.contains(contains());
      }
      return regEx().matches(args);
    }

    private static StringMatcher create(@Nullable String exact, @Nullable String prefix,
        @Nullable String suffix, @Nullable Pattern regEx, @Nullable String contains,
        boolean ignoreCase) {
      return new AutoValue_Matcher_StringMatcher(exact, prefix, suffix, regEx, contains,
          ignoreCase);
    }
  }

  /** Evaluate whether request path matches a string matcher pattern. */
  static class PathMatcher extends Matcher {
    private final StringMatcher delegate;

    public PathMatcher(StringMatcher stringMatcher) {
      this.delegate = stringMatcher;
    }

    @Override
    public boolean matches(EvaluateArgs args) {
      return delegate.matches(args.getFullMethodName());
    }
  }

  /** Evaluate whether request principal argument matches a string matcher pattern. */
  static class AuthenticatedMatcher extends Matcher {
    private final StringMatcher delegate;

    public AuthenticatedMatcher(StringMatcher stringMatcher) {
      this.delegate = stringMatcher;
    }

    @Override
    public boolean matches(EvaluateArgs args) {
      return delegate.matches(args.getPrincipalName());
    }
  }

  /** Evaluate whether the request destination address matches an IP address pattern. */
  static class DestinationIpMatcher extends Matcher {
    private final IpMatcher delegate;

    public DestinationIpMatcher(IpMatcher ipMatcher) {
      this.delegate = ipMatcher;
    }

    @Override
    public boolean matches(EvaluateArgs args) {
      return delegate.matches(args.getDestinationAddress());
    }
  }

  /** Evaluate whether the request source address matches an IP address pattern. */
  static class SourceIpMatcher extends Matcher {
    private final IpMatcher delegate;

    public SourceIpMatcher(IpMatcher ipMatcher) {
      this.delegate = ipMatcher;
    }

    @Override
    public boolean matches(EvaluateArgs args) {
      return delegate.matches(args.getSourceAddress());
    }
  }

  /** Evaluates whether an IPv4 or IPv6 address is within a CIDR range.  */
  @AutoValue
  abstract static class IpMatcher extends Matcher {
    @Nullable
    abstract String addressPrefix();

    abstract int prefixLen();

    @Override
    public boolean matches(EvaluateArgs args) {
      throw new UnsupportedOperationException("error");
    }

    public boolean matches(String args) {
      int len = prefixLen();
      byte[] ip;
      byte[] subnet;
      try {
        ip = InetAddress.getByName(args).getAddress();
        subnet = InetAddress.getByName(addressPrefix()).getAddress();
      } catch (Exception ex) {
        return false;
      }
      if (ip.length != subnet.length) {
        return false;
      }
      for (int i = 0; i < ip.length; i++) {
        int mask = 256 - (1 << (len >= 8 ? 0 : len % 8));
        if ((mask & ip[i]) != (mask & subnet[i]))  {
          return false;
        }
        len -= 8;
        if (len <= 0) {
          return true;
        }
      }
      return true;
    }

    static IpMatcher create(String addressPrefix, int prefixLen) {
      return new AutoValue_Matcher_IpMatcher(addressPrefix, prefixLen);
    }
  }
}
