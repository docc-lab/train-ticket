package travel2.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
public class Travel2ServiceConfiguration {
    
    @Bean
    public RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3000);  // 3 seconds
        factory.setReadTimeout(5000);     // 5 seconds
        return new RestTemplate(factory);
    }

    @Bean
    public IRule ribbonRule() {
        return new RetryRule(new RoundRobinRule()); // Add retry with round-robin
    }
    
    @Bean
    public ILoadBalancer ribbonLoadBalancer() {
        return new ZoneAwareLoadBalancer<>(); // Zone aware load balancing
    }
}