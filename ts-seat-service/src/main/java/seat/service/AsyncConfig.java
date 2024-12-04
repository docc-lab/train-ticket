package seat.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskDecorator;
import org.apache.skywalking.apm.toolkit.trace.RunnableWrapper;
import org.apache.skywalking.apm.toolkit.trace.TraceContext;
import org.apache.skywalking.apm.toolkit.trace.Tracer;
import org.apache.skywalking.apm.toolkit.trace.ContextSnapshotRef;
import org.apache.skywalking.apm.toolkit.trace.SpanRef;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.client.RestTemplate;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.boot.web.client.RestTemplateBuilder;

@Configuration
public class AsyncConfig {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncConfig.class);
    
    @Bean
    public TaskDecorator traceContextDecorator() {
        return runnable -> {
            // Capture context from parent thread
            String parentTraceId = TraceContext.traceId();
            String parentSegmentId = TraceContext.segmentId();
            RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
            ContextSnapshotRef contextSnapshot = Tracer.capture();
            
            return RunnableWrapper.of(() -> {
                try {
                    // Restore request context
                    RequestContextHolder.setRequestAttributes(requestAttributes);
                    
                    // Continue trace context in worker thread
                    Tracer.continued(contextSnapshot);
                    
                    // Create span for async work
                    SpanRef asyncSpan = Tracer.createLocalSpan("async.task");
                    asyncSpan.tag("parent.traceId", parentTraceId);
                    asyncSpan.tag("parent.segmentId", parentSegmentId);
                    asyncSpan.tag("async.thread", Thread.currentThread().getName());
                    
                    // Log context continuation
                    String currentTraceId = TraceContext.traceId();
                    LOGGER.debug("[AsyncTask][Context continued][Parent TraceID: {}][Current TraceID: {}][Thread: {}]",
                        parentTraceId, currentTraceId, Thread.currentThread().getName());
                    
                    runnable.run();
                    
                } finally {
                    RequestContextHolder.resetRequestAttributes();
                    Tracer.stopSpan();
                }
            });
        };
    }

    @LoadBalanced
    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        RestTemplate template = builder.build();
        template.getInterceptors().add((request, body, execution) -> {
            // Get current trace context
            String traceId = TraceContext.traceId();
            String segmentId = TraceContext.segmentId();
            
            // Get SkyWalking carrier
            ContextCarrierRef carrier = new ContextCarrierRef();
            Tracer.inject(carrier);
            
            LOGGER.debug("[RestTemplate][Injecting trace context][TraceId: {}][SegmentId: {}]", 
                traceId, segmentId);
                
            // Add context propagation headers
            CarrierItemRef item = carrier.items(); 
            while (item.hasNext()) {
                item = item.next();
                request.getHeaders().set(item.getHeadKey(), item.getHeadValue());
            }
            
            // Add additional SW8 trace headers
            request.getHeaders().set("sw8-correlation", traceId);
            
            return execution.execute(request, body);
        });
        return template;
    }
}