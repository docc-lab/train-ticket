package travel.service;

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

    @Bean
    public RestTemplate restTemplate() {
        RestTemplate template = new RestTemplate();
        template.getInterceptors().add((request, body, execution) -> {
            String traceId = TraceContext.traceId();
            LOGGER.debug("[RestTemplate][Adding trace headers][TraceID: {}]", traceId);
            request.getHeaders().set("sw8", traceId);
            request.getHeaders().set("sw8-correlation", traceId);
            return execution.execute(request, body);
        });
        return template;
    }
}