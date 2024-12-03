package fdse.microservice.service;

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
                ActiveSpan.tag("parent.traceId", parentTraceId);
                runnable.run();
            });
        };
    }
}