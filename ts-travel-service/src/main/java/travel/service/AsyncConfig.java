package travel.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskDecorator;
import org.apache.skywalking.apm.toolkit.trace.CallableWrapper;
import org.apache.skywalking.apm.toolkit.trace.RunnableWrapper;
import org.apache.skywalking.apm.toolkit.trace.TraceContext;
import org.apache.skywalking.apm.toolkit.trace.ActiveSpan;
import org.apache.skywalking.apm.toolkit.trace.ContextCarrierRef;
import org.apache.skywalking.apm.toolkit.trace.Tracer;
import org.apache.skywalking.apm.toolkit.trace.ContextSnapshotRef;
import org.apache.skywalking.apm.toolkit.trace.SpanRef;


@Configuration
public class AsyncConfig {
    @Bean
    public TaskDecorator traceContextDecorator() {
        return runnable -> {
            // Capture all current context information
            String parentTraceId = TraceContext.traceId();
            ContextSnapshotRef contextSnapshot = Tracer.capture();
            
            return RunnableWrapper.of(() -> {
                SpanRef asyncSpan = null;
                try {
                    // Create new isolated span for async work
                    asyncSpan = Tracer.createLocalSpan("async.task");
                    // Link to parent context
                    Tracer.continued(contextSnapshot);
                    ActiveSpan.tag("parent.traceId", parentTraceId);
                    ActiveSpan.tag("async.thread", Thread.currentThread().getName());
                    
                    runnable.run();
                } finally {
                    if (asyncSpan != null) {
                        Tracer.stopSpan();
                    }
                }
            });
        };
    }
}