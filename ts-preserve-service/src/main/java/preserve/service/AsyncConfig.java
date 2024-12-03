package preserve.service;

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
            // Capture current trace context and thread info for better isolation
            String parentTraceId = TraceContext.traceId();
            String threadId = Thread.currentThread().getName();
            
            return RunnableWrapper.of(() -> {
                try {
                    // Add thread-specific context to help isolate concurrent requests
                    ActiveSpan.tag("thread.id", threadId);
                    ActiveSpan.tag("parent.traceId", parentTraceId);
                    
                    runnable.run();
                } catch (Exception e) {
                    ActiveSpan.tag("error", "true");
                    ActiveSpan.tag("error.msg", e.getMessage());
                    throw e;
                }
            });
        };
    }

    @Bean
    public ThreadPoolTaskExecutor asyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(20);  // Handle concurrent requests
        executor.setMaxPoolSize(40);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("preserve-async-");
        executor.setTaskDecorator(traceContextDecorator());
        return executor;
    }
}