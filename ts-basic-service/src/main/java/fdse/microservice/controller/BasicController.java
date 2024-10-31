package fdse.microservice.controller;

import edu.fudan.common.entity.Travel;
import fdse.microservice.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.time.Instant;

import static org.springframework.http.ResponseEntity.ok;

/**
 * @author Chenjie
 * @date 2017/6/6.
 */
@RestController
@RequestMapping("/api/v1/basicservice")

public class BasicController {

    @Autowired
    BasicService service;

    private static final int BURST_REQUESTS_PER_SEC = 5;    // Bursty load size in rqs
    private static final int BURST_DURATION_SECONDS = 10;   // Duration of one burst wave
    private static final int BURSTY_PERIOD_SECONDS = 60;    // Interval between burst waves
    private static final int THREAD_POOL_SIZE = Math.max(1, BURST_REQUESTS_PER_SEC * 2);

    private final ExecutorService executorService;
    private final ScheduledExecutorService schedulerService;
    private final AtomicLong lastBurstTime = new AtomicLong(0);

    private static final Logger logger = LoggerFactory.getLogger(BasicController.class);

    @Autowired
    public BasicController(BasicService service) {
        logger.info("Initializing BasicController - Burst Config: {} requests/sec for {} seconds, repeating every {} seconds",
                   BURST_REQUESTS_PER_SEC, BURST_DURATION_SECONDS, BURSTY_PERIOD_SECONDS);
        this.service = service;
        this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        this.schedulerService = Executors.newScheduledThreadPool(1);
        logger.info("BasicController initialization completed");
    }

    @GetMapping(path = "/welcome")
    public String home(@RequestHeader HttpHeaders headers) {
        return "Welcome to [ Basic Service ] !";
    }

    @PostMapping(value = "/basic/travel")
    public HttpEntity queryForTravel(@RequestBody Travel info, @RequestHeader HttpHeaders headers) {
        // TravelResult
        logger.info("[queryForTravel][Query for travel][Travel: {}]", info.toString());
        // return ok(service.queryForTravel(info, headers));
        try {
            // Get the actual response
            Object response = service.queryForTravel(info, headers);

            // Check if it's time for a new burst wave
            long currentTime = Instant.now().getEpochSecond();
            long lastBurst = lastBurstTime.get();
            
            if (currentTime - lastBurst >= BURSTY_PERIOD_SECONDS && 
                lastBurstTime.compareAndSet(lastBurst, currentTime)) {
                logger.info("[queryForTravel][Triggering new burst after {} seconds since last burst]", 
                          currentTime - lastBurst);
                generateBurstLoad(info, headers);
            } else if (currentTime - lastBurst < BURSTY_PERIOD_SECONDS) {
                logger.debug("[queryForTravel][Skipping burst][{} seconds remaining in bursty period]", 
                          BURSTY_PERIOD_SECONDS - (currentTime - lastBurst));
            }

            return ok(response);
        } catch (Exception e) {
            logger.error("[queryForTravel][Error in main request][Error: {}]", e.getMessage());
            return ok(null);
        }
    }

    @PostMapping(value = "/basic/travels")
    public HttpEntity queryForTravels(@RequestBody List<Travel> infos, @RequestHeader HttpHeaders headers) {
        // TravelResult
        logger.info("[queryForTravels][Query for travels][Travels: {}]", infos);
        return ok(service.queryForTravels(infos, headers));
    }

    @GetMapping(value = "/basic/{stationName}")
    public HttpEntity queryForStationId(@PathVariable String stationName, @RequestHeader HttpHeaders headers) {
        // String id
        logger.info("[queryForStationId][Query for stationId by stationName][stationName: {}]", stationName);
        return ok(service.queryForStationId(stationName, headers));
    }

    private void generateBurstLoad(String orderId, String loginId, HttpHeaders headers) {
        LOGGER.info("[generateBurstLoad][Starting burst: {} requests/sec for {} seconds. Next burst in {} seconds]", 
                   BURST_REQUESTS_PER_SEC, BURST_DURATION_SECONDS, BURSTY_PERIOD_SECONDS);

        // Schedule fixed-rate bursts for the duration
        ScheduledFuture<?> burstSchedule = schedulerService.scheduleAtFixedRate(() -> {
            // Submit all requests for this second instantly
            for (int i = 0; i < BURST_REQUESTS_PER_SEC; i++) {
                executorService.submit(() -> {
                    try {
                        cancelService.cancelOrder(orderId, loginId, headers);
                    } catch (Exception e) {
                        LOGGER.warn("[generateBurstLoad][Burst request failed][Error: {}]", e.getMessage());
                    }
                });
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);

        // Schedule the burst termination
        schedulerService.schedule(() -> {
            burstSchedule.cancel(false);
            LOGGER.info("[generateBurstLoad][Burst completed][Next burst possible in {} seconds]", 
                       BURSTY_PERIOD_SECONDS);
        }, BURST_DURATION_SECONDS, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdownExecutorServices() {
        LOGGER.info("Shutting down executor services");
        executorService.shutdown();
        schedulerService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
            if (!schedulerService.awaitTermination(60, TimeUnit.SECONDS)) {
                schedulerService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            schedulerService.shutdownNow();
            Thread.currentThread().interrupt();
            LOGGER.error("Shutdown interrupted", e);
        }
    }

}
