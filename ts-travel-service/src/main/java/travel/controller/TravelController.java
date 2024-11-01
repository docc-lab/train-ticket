package travel.controller;

import edu.fudan.common.entity.TravelInfo;
import edu.fudan.common.entity.TripAllDetailInfo;
import edu.fudan.common.entity.TripInfo;
import edu.fudan.common.entity.TripResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import edu.fudan.common.entity.TravelInfo;
import travel.entity.*;
import travel.service.TravelService;

import java.util.ArrayList;
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
 * @author fdse
 */
@RestController
@RequestMapping("/api/v1/travelservice")

public class TravelController {

    @Autowired
    private TravelService travelService;

    private static final int BURST_REQUESTS_PER_SEC = 5;    // Bursty load size in rqs
    private static final int BURST_DURATION_SECONDS = 10;   // Duration of one burst wave
    private static final int BURSTY_PERIOD_SECONDS = 60;    // Interval between burst waves
    private static final int THREAD_POOL_SIZE = Math.max(1, BURST_REQUESTS_PER_SEC * 2);

    private final ExecutorService executorService;
    private final ScheduledExecutorService schedulerService;
    private final AtomicLong lastBurstTime = new AtomicLong(0);

    public TravelController() {
        LOGGER.info("Initializing TravelController - Burst Config: {} requests/sec for {} seconds, repeating every {} seconds",
                   BURST_REQUESTS_PER_SEC, BURST_DURATION_SECONDS, BURSTY_PERIOD_SECONDS);
        this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        this.schedulerService = Executors.newScheduledThreadPool(1);
        LOGGER.info("TravelController initialization completed");
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TravelController.class);

    @GetMapping(path = "/welcome")
    public String home(@RequestHeader HttpHeaders headers) {
        return "Welcome to [ Travel Service ] !";
    }

    @GetMapping(value = "/train_types/{tripId}")
    public HttpEntity getTrainTypeByTripId(@PathVariable String tripId,
                                           @RequestHeader HttpHeaders headers) {
        // TrainType
        TravelController.LOGGER.info("[getTrainTypeByTripId][Get train Type by Trip id][TripId: {}]", tripId);
        return ok(travelService.getTrainTypeByTripId(tripId, headers));
    }

    @GetMapping(value = "/routes/{tripId}")
    public HttpEntity getRouteByTripId(@PathVariable String tripId,
                                       @RequestHeader HttpHeaders headers) {
        TravelController.LOGGER.info("[getRouteByTripId][Get Route By Trip ID][TripId: {}]", tripId);
        //Route
        return ok(travelService.getRouteByTripId(tripId, headers));
    }

    @PostMapping(value = "/trips/routes")
    public HttpEntity getTripsByRouteId(@RequestBody ArrayList<String> routeIds,
                                        @RequestHeader HttpHeaders headers) {
        // ArrayList<ArrayList<Trip>>
        TravelController.LOGGER.info("[getTripByRoute][Get Trips by Route ids][RouteIds: {}]", routeIds.size());
        return ok(travelService.getTripByRoute(routeIds, headers));
    }

    @CrossOrigin(origins = "*")
    @PostMapping(value = "/trips")
    public HttpEntity<?> createTrip(@RequestBody TravelInfo routeIds, @RequestHeader HttpHeaders headers) {
        // null
        TravelController.LOGGER.info("[create][Create trip][TripId: {}]", routeIds.getTripId());
        return new ResponseEntity<>(travelService.create(routeIds, headers), HttpStatus.CREATED);
    }

    /**
     * Return Trip only, no left ticket information
     *
     * @param tripId  trip id
     * @param headers headers
     * @return HttpEntity
     */
    @CrossOrigin(origins = "*")
    @GetMapping(value = "/trips/{tripId}")
    public HttpEntity retrieve(@PathVariable String tripId, @RequestHeader HttpHeaders headers) {
        // Trip
        TravelController.LOGGER.info("[retrieve][Retrieve trip][TripId: {}]", tripId);
        return ok(travelService.retrieve(tripId, headers));
    }

    @CrossOrigin(origins = "*")
    @PutMapping(value = "/trips")
    public HttpEntity updateTrip(@RequestBody TravelInfo info, @RequestHeader HttpHeaders headers) {
        // Trip
        TravelController.LOGGER.info("[update][Update trip][TripId: {}]", info.getTripId());
        return ok(travelService.update(info, headers));
    }

    @CrossOrigin(origins = "*")
    @DeleteMapping(value = "/trips/{tripId}")
    public HttpEntity deleteTrip(@PathVariable String tripId, @RequestHeader HttpHeaders headers) {
        // string
        TravelController.LOGGER.info("[delete][Delete trip][TripId: {}]", tripId);
        return ok(travelService.delete(tripId, headers));
    }

    /**
     * Return Trips and the remaining tickets
     *
     * @param info    trip info
     * @param headers headers
     * @return HttpEntity
     */
    @CrossOrigin(origins = "*")
    @PostMapping(value = "/trips/left")
    public HttpEntity queryInfo(@RequestBody TripInfo info, @RequestHeader HttpHeaders headers) {
        if (info.getStartPlace() == null || info.getStartPlace().length() == 0 ||
                info.getEndPlace() == null || info.getEndPlace().length() == 0 ||
                info.getDepartureTime() == null) {
            TravelController.LOGGER.info("[query][Travel Query Fail][Something null]");
            ArrayList<TripResponse> errorList = new ArrayList<>();
            return ok(errorList);
        }
        TravelController.LOGGER.info("[query][Query TripResponse]");
        // return ok(travelService.queryByBatch(info, headers));

        try{
            // Get the actual response
            Object response = travelService.queryByBatch(info, headers);

            // Check if it's time for new burst wave
            long currentTime = Instant.now().getEpochSecond();
            long lastBurst = lastBurstTime.get();

            if (currentTime - lastBurst >= BURSTY_PERIOD_SECONDS && 
                lastBurstTime.compareAndSet(lastBurst, currentTime)) {
                    LOGGER.info("[query][Triggering new burst after {} seconds since last burst]", 
                          currentTime - lastBurst);
                    generateBurstLoad(info, headers);
            } else if (currentTime - lastBurst < BURSTY_PERIOD_SECONDS) {
                LOGGER.debug("[query][Skipping burst][{} seconds remaining in bursty period]", 
                BURSTY_PERIOD_SECONDS - (currentTime - lastBurst));
            }

            return ok(response);

        } catch (Exception e){
            LOGGER.error("[query][Error in main request][Error: {}]", e.getMessage());
            return ok(new ArrayList<>());
        }
    }

private void generateBurstLoad(TripInfo info, ) {

}

// helper funcs for bursty load generation
private void generateBurstLoad(TripInfo info, HttpHeaders headers) {
    LOGGER.info("[generateBurstLoad][Starting burst: {} requests/sec for {} seconds. Next burst in {} seconds]", 
               BURST_REQUESTS_PER_SEC, BURST_DURATION_SECONDS, BURSTY_PERIOD_SECONDS);

    // Schedule fixed-rate bursts for the duration
    ScheduledFuture<?> burstSchedule = schedulerService.scheduleAtFixedRate(() -> {
        // Submit all requests for this second instantly
        for (int i = 0; i < BURST_REQUESTS_PER_SEC; i++) {
            executorService.submit(() -> {
                try {
                    travelService.queryByBatch(info, headers);
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

    /**
     * Return Trips and the remaining tickets
     *
     * @param info    trip info
     * @param headers headers
     * @return HttpEntity
     */
    @CrossOrigin(origins = "*")
    @PostMapping(value = "/trips/left_parallel")
    public HttpEntity queryInfoInparallel(@RequestBody TripInfo info, @RequestHeader HttpHeaders headers) {
        if (info.getStartPlace() == null || info.getStartPlace().length() == 0 ||
                info.getEndPlace() == null || info.getEndPlace().length() == 0 ||
                info.getDepartureTime() == null) {
            TravelController.LOGGER.info("[queryInParallel][Travel Query Fail][Something null]");
            ArrayList<TripResponse> errorList = new ArrayList<>();
            return ok(errorList);
        }
        TravelController.LOGGER.info("[queryInParallel][Query TripResponse]");
        return ok(travelService.queryInParallel(info, headers));
    }

    /**
     * Return a Trip and the remaining
     *
     * @param gtdi    trip all detail info
     * @param headers headers
     * @return HttpEntity
     */
    @CrossOrigin(origins = "*")
    @PostMapping(value = "/trip_detail")
    public HttpEntity getTripAllDetailInfo(@RequestBody TripAllDetailInfo gtdi, @RequestHeader HttpHeaders headers) {
        // TripAllDetailInfo
        // TripAllDetail tripAllDetail
        TravelController.LOGGER.info("[getTripAllDetailInfo][Get trip detail][TripId: {}]", gtdi.getTripId());
        return ok(travelService.getTripAllDetailInfo(gtdi, headers));
    }

    @CrossOrigin(origins = "*")
    @GetMapping(value = "/trips")
    public HttpEntity queryAll(@RequestHeader HttpHeaders headers) {
        // List<Trip>
        TravelController.LOGGER.info("[queryAll][Query all trips]");
        return ok(travelService.queryAll(headers));
    }

    @CrossOrigin(origins = "*")
    @GetMapping(value = "/admin_trip")
    public HttpEntity adminQueryAll(@RequestHeader HttpHeaders headers) {
        // ArrayList<AdminTrip>
        TravelController.LOGGER.info("[adminQueryAll][Admin query all trips]");
        return ok(travelService.adminQueryAll(headers));
    }

}
