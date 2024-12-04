package travel.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskDecorator;
import org.apache.skywalking.apm.toolkit.trace.CallableWrapper;
import org.apache.skywalking.apm.toolkit.trace.RunnableWrapper;
import org.apache.skywalking.apm.toolkit.trace.TraceContext;
import org.apache.skywalking.apm.toolkit.trace.ActiveSpan;
import org.apache.skywalking.apm.toolkit.trace.ContextCarrierRef;

@Configuration
public class AsyncConfig {

    @Bean
    public TaskDecorator traceContextDecorator() {
        return runnable -> {
            String parentTraceId = TraceContext.traceId();
            ContextSnapshotRef contextSnapshot = Tracer.capture();
            
            return RunnableWrapper.of(() -> {
                try {
                    // Restore the trace context
                    Tracer.continued(contextSnapshot);
                    ActiveSpan.tag("parent.traceId", parentTraceId);
                    runnable.run();
                } catch (Exception e) {
                    ActiveSpan.tag("error", "true");
                    ActiveSpan.tag("error.message", e.getMessage());
                    throw e;
                }
            });
        };
    }
}