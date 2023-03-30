package io.grpc.census;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static io.grpc.Metadata.BINARY_BYTE_MARSHALLER;

import com.google.common.collect.ImmutableList;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientStreamTracer;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;
import io.opencensus.trace.Tracing;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

public class OpenTelemetryTracingModule {
  private static Logger log = Logger.getLogger(OpenTelemetryTracingModule.class.getName());
  private static final TextMapSetter<Metadata> gRPCTextMapSetter = new CustomTextMapSetter();
  private CustomTextMapGetter gRPCTextMapGetter = new CustomTextMapGetter();
  private Tracer tracer;
  private static final GrpcTraceBinTextMapPropagator grpcTraceBinPropagator =
      new GrpcTraceBinTextMapPropagator();
  private final ClientInterceptor interceptor =
      new TracingClientInterceptor(grpcTraceBinPropagator);

  private final ServerStreamTracer.Factory serverStreamTracerFactory =
      new ServerTracerFactory(grpcTraceBinPropagator);

  public OpenTelemetryTracingModule() {
    SpanExporter exporter = LoggingSpanExporter.create();
    SdkTracerProvider sdkTracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .setSampler(Sampler.alwaysOn())
            .build();
    OpenTelemetrySdk openTelemetrySdk =
        OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setPropagators(ContextPropagators.create(new GrpcTraceBinTextMapPropagator()))
            .buildAndRegisterGlobal();
    this.tracer = openTelemetrySdk.getTracer("grpc-opentelemetry-tracing");
  }

  public ClientInterceptor getInterceptor() {
    return interceptor;
  }

  private static class ClientTracer extends ClientStreamTracer {
    TextMapPropagator compositePropagator;
    Context context;

    Span span;

    ClientTracer(List<TextMapPropagator> propagators, Context context, Span span) {
      this.compositePropagator = TextMapPropagator.composite(propagators);
      this.context= context;
      this.span = span;
    }
    @Override
    public void streamCreated(Attributes attributes, Metadata headers) {
      compositePropagator.inject(context, headers, gRPCTextMapSetter);
    }

    @Override
    public void streamClosed(Status status) {
      span.end();
    }
  }

  public class TracingClientInterceptor implements ClientInterceptor {
    List<TextMapPropagator> propagators;

    public TracingClientInterceptor(TextMapPropagator... propagators) {
      this.propagators = ImmutableList.copyOf(propagators);
    }
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions,
                                                               Channel next) {
      Span rootSpan = tracer.spanBuilder("root").startSpan();
      log.log(Level.INFO, "root client trace: " + rootSpan.toString());
      Context context = Context.current().with(Span.wrap(rootSpan.getSpanContext()));
      return next.newCall(method, callOptions.withStreamTracerFactory(new ClientStreamTracer.Factory() {
        @Override
        public ClientStreamTracer newClientStreamTracer(ClientStreamTracer.StreamInfo info, Metadata headers) {
          return new ClientTracer(propagators, context, rootSpan);
        }
      }));
    }
  }

  private static class BinaryFormat {
    static SpanContext parseBytes(byte[] value) {
      try {
        //todo: https://github.com/census-instrumentation/opencensus-java/blob/3d7f0b511dea392bbf68ee5268dd95857552dd87/impl_core/src/main/java/io/opencensus/implcore/trace/propagation/BinaryFormatImpl.java
        io.opencensus.trace.SpanContext ocContext =
            Tracing.getPropagationComponent().getBinaryFormat().fromByteArray(value);
        log.log(Level.INFO, "parse bytes to oc context " + ocContext);

        return SpanContext.create(ocContext.getTraceId().toLowerBase16(),
            ocContext.getSpanId().toLowerBase16(),
            TraceFlags.getDefault(), TraceState.getDefault());

      } catch (Exception ex) {
        return null;
      }
    }
    static byte[] toBytes(SpanContext spanContext) {
      byte[] spanBytes = spanContext.getSpanIdBytes();
      byte[] traceBytes = spanContext.getTraceIdBytes();
      byte[] bytes = new byte[spanBytes.length + traceBytes.length + 1];
      System.arraycopy(spanBytes, 0, bytes, 0, spanBytes.length);
      System.arraycopy(traceBytes, 0, bytes, spanBytes.length, traceBytes.length);
      System.arraycopy(new byte[]{spanContext.getTraceFlags().asByte()}, 0, bytes, spanBytes.length + traceBytes.length, 1);
      return bytes;
    }
  }


  public static class GrpcTraceBinTextMapPropagator implements TextMapPropagator {
   @Override
   public Collection<String> fields() {
     return null;
   }
   @Override public <C> void inject(Context context, @Nullable C carrier, TextMapSetter<C> setter) {
     SpanContext spanContext = Span.fromContext(context).getSpanContext();
     byte[] value = BinaryFormat.toBytes(spanContext);
     if (setter instanceof CustomTextMapSetter) {
       ((CustomTextMapSetter) setter).set((Metadata) carrier, "grpc-trace-bin", value);
     } else {
       setter.set(carrier, "grpc-trace-bin", Base64.getEncoder().encodeToString(value));
     }
   }

   @Override public <C> Context extract(Context context, @Nullable C c, TextMapGetter<C> textMapGetter) {
     byte[] bytes;
     if (textMapGetter instanceof CustomTextMapGetter) {
       bytes = ((CustomTextMapGetter) textMapGetter).getBinary((Metadata) c, "grpc-trace-bin");
     } else {
       String contextString = textMapGetter.get(c, "grpc-trace-bin");
       bytes = Base64.getDecoder().decode(contextString);
     }
     SpanContext spanContext = BinaryFormat.parseBytes(bytes);
     return context.with(Span.wrap(spanContext));
   }
 }

   // important, we provide
   private static final class CustomTextMapSetter implements TextMapSetter<Metadata>,
       BinarySetter<Metadata> {
     @Override
     public void set(@Nullable Metadata metadata, String key, String value) {
       metadata.put(Metadata.Key.of(key, ASCII_STRING_MARSHALLER), value);
     }

     @Override
     public void set(@Nullable Metadata metadata, String key, byte[] value) {
       metadata.put(Metadata.Key.of(key, BINARY_BYTE_MARSHALLER), value);
     }
   }

   private interface BinarySetter<C> {
    void set(@Nullable C carrier, String key, byte[] value);
   }

   private interface BinaryGetter<C> {
     byte[] getBinary(@Nullable C carrier, String key);
   }

   private static class CustomTextMapGetter implements TextMapGetter<Metadata>, BinaryGetter<Metadata> {

     @Override
     public Iterable<String> keys(Metadata carrier) {
       return carrier.keys();
     }

     @Nullable
     @Override
     public String get(@Nullable Metadata carrier, String key) {
       if (carrier != null) {
         return carrier.get(Metadata.Key.of(key, ASCII_STRING_MARSHALLER));
       }
       return null;
     }
     @Override
     public byte[] getBinary(@Nullable Metadata carrier, String key) {
       return carrier.get(Metadata.Key.of(key, BINARY_BYTE_MARSHALLER));
     }
   }

   final class ServerTracerFactory extends ServerStreamTracer.Factory {
    TextMapPropagator propagator;

    public ServerTracerFactory(TextMapPropagator... propagators) {
      this.propagator = TextMapPropagator.composite(propagators);
    }
     @Override
     public ServerStreamTracer newServerStreamTracer(String fullMethodName, Metadata headers) {
      return new ServerTracer(propagator.extract(Context.current(), headers, gRPCTextMapGetter));
     }
   }
   private final class ServerTracer extends ServerStreamTracer {
     private final Span span;

     ServerTracer(Context remoteContext) {
       this.span = tracer.spanBuilder("grpc-server").setParent(remoteContext).startSpan();
       log.log(Level.INFO, "server span:" + span);
     }

     @Override
     public void streamClosed(Status status) {
       span.end();
       log.log(Level.INFO, "server span closed");
     }
   }

   public ServerStreamTracer.Factory getStreamTracerFactory() {
    return serverStreamTracerFactory;
   }
}
