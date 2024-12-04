package seat.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskDecorator;
import org.apache.skywalking.apm.toolkit.trace.RunnableWrapper;
import org.apache.skywalking.apm.toolkit.trace.TraceContext;
import org.apache.skywalking.apm.toolkit.trace.ActiveSpan;
import org.apache.skywalking.apm.toolkit.trace.ContextCarrierRef;
import org.apache.skywalking.apm.toolkit.trace.Tracer;
import org.apache.skywalking.apm.toolkit.trace.ContextSnapshotRef;
import org.apache.skywalking.apm.toolkit.trace.SpanRef;

@Configuration
public class AsyncConfig {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncConfig.class);
    
    @Bean
    public TaskDecorator traceContextDecorator() {
        return runnable -> {
            // Capture all current context information
            String parentTraceId = TraceContext.traceId();
            String parentSegmentId = TraceContext.segmentId();
            ContextSnapshotRef contextSnapshot = Tracer.capture();
            
            // Create carrier for potential downstream calls
            ContextCarrierRef carrier = new ContextCarrierRef();
            Tracer.inject(carrier);
            
            // Store all carrier items for propagation
            Map<String, String> contextItems = new HashMap<>();
            CarrierItemRef item = carrier.items();
            while (item.hasNext()) {
                item = item.next();
                contextItems.put(item.getHeadKey(), item.getHeadValue());
            }
            
            return RunnableWrapper.of(() -> {
                SpanRef asyncSpan = null;
                try {
                    // Continue the trace context in the new thread
                    Tracer.continued(contextSnapshot);
                    
                    // Create new span for async work
                    asyncSpan = Tracer.createLocalSpan("async.task");
                    asyncSpan.tag("parent.traceId", parentTraceId);
                    asyncSpan.tag("parent.segmentId", parentSegmentId);
                    asyncSpan.tag("async.thread", Thread.currentThread().getName());
                    
                    // Log context continuation
                    LOGGER.debug("[AsyncTask][Context continued][TraceID: {}][Thread: {}]",
                        TraceContext.traceId(), Thread.currentThread().getName());
                    
                    // Execute the actual task
                    runnable.run();
                    
                } catch (Exception e) {
                    if (asyncSpan != null) {
                        asyncSpan.log(e);
                        asyncSpan.tag("error", "true");
                        asyncSpan.tag("error.message", e.getMessage());
                    }
                    LOGGER.error("[AsyncTask][Execution failed][TraceID: {}][Error: {}]",
                        TraceContext.traceId(), e.getMessage());
                    throw e;
                } finally {
                    if (asyncSpan != null) {
                        // Ensure span is always closed
                        Tracer.stopSpan();
                    }
                }
            });
        };
    }
}