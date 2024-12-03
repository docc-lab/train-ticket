package preserve.service;

import org.apache.skywalking.apm.toolkit.trace.TraceContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;

public class TraceContextHelper {
    
    public static HttpHeaders enrichHeaders(HttpHeaders headers) {
        HttpHeaders enrichedHeaders = new HttpHeaders();
        if (headers != null) {
            enrichedHeaders.putAll(headers);
        }
        
        String currentTraceId = TraceContext.traceId();
        String threadId = Thread.currentThread().getName();
        
        if (currentTraceId != null) {
            // Add thread-specific trace context
            enrichedHeaders.set("sw8", currentTraceId + "." + threadId);
            // Preserve original trace context as parent
            enrichedHeaders.set("sw8-parent", currentTraceId);
        }
        
        return enrichedHeaders;
    }

    public static HttpEntity<?> createRequestEntity(Object body, HttpHeaders headers) {
        return new HttpEntity<>(body, enrichHeaders(headers));
    }
}