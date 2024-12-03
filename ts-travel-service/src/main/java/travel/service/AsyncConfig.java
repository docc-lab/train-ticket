package travel.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskDecorator;
import org.apache.skywalking.apm.toolkit.trace.RunnableWrapper;
import org.apache.skywalking.apm.toolkit.trace.TraceContext;
import org.apache.skywalking.apm.toolkit.trace.ActiveSpan;

@Configuration
public class AsyncConfig {

    @Bean
    public TaskDecorator traceContextDecorator() {
        return runnable -> {
            String parentTraceId = TraceContext.traceId();
            return RunnableWrapper.of(() -> {
                try {
                    // Create new span with explicit parent reference
                    ActiveSpan.tag("parent.traceId", parentTraceId);
                    ActiveSpan.tag("cross_process.context", "burst-request");
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