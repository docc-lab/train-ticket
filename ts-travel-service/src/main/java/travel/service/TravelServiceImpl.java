package travel.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.fudan.common.entity.*;
import edu.fudan.common.util.JsonUtils;
import edu.fudan.common.util.Response;
import edu.fudan.common.util.StringUtils;
import org.apache.skywalking.apm.toolkit.trace.TraceCrossThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import travel.entity.AdminTrip;
import travel.entity.Travel;
import travel.entity.Trip;
import travel.entity.TripAllDetail;
import travel.repository.TripRepository;

import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.core.task.TaskDecorator;
import org.apache.skywalking.apm.toolkit.trace.*;
import org.apache.skywalking.apm.toolkit.trace.ActiveSpan;
import org.apache.skywalking.apm.toolkit.trace.CallableWrapper;
import org.apache.skywalking.apm.toolkit.trace.RunnableWrapper;
import org.apache.skywalking.apm.toolkit.trace.TraceContext;
import org.apache.skywalking.apm.toolkit.trace.ContextCarrierRef;

import javax.transaction.Transactional;
import java.util.*;
import java.util.concurrent.*;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author fdse
 */
@Service
public class TravelServiceImpl implements TravelService {

    private static final int BURST_REQUESTS_PER_SEC = 10;
    private static final int BURST_DURATION_SECONDS = 10;
    private static final int BURST_PERIOD_SECONDS = 60;
    private static final int BURST_BUFFER = 2;  // Buffer multiplier
    private static final int THREAD_POOL_SIZE = Math.max(BURST_REQUESTS_PER_SEC * BURST_DURATION_SECONDS * BURST_BUFFER, 
                                                        Runtime.getRuntime().availableProcessors() * 2);
    
    // Executors for burst handling
    private ThreadPoolTaskExecutor taskExecutor;
    private ThreadPoolTaskScheduler taskScheduler;
    private static final AtomicLong lastBurstTime = new AtomicLong(0);

    @Autowired
    private TaskDecorator traceContextDecorator;

    @Autowired
    private TripRepository repository;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private DiscoveryClient discoveryClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(TravelServiceImpl.class);

    private static final ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomizableThreadFactory("HttpClientThreadPool-"));

    private String getServiceUrl(String serviceName) {
        return "http://" + serviceName;
    }

    String success = "Success";
    String noContent = "No Content";

    @PostConstruct
    public void init() {
        this.taskExecutor = new ThreadPoolTaskExecutor();
        this.taskExecutor.setCorePoolSize(BURST_REQUESTS_PER_SEC);
        this.taskExecutor.setMaxPoolSize(THREAD_POOL_SIZE);
        this.taskExecutor.setQueueCapacity(BURST_REQUESTS_PER_SEC * BURST_DURATION_SECONDS);
        this.taskExecutor.setThreadNamePrefix("travel-burst-worker-");
        this.taskExecutor.setTaskDecorator(traceContextDecorator);
        this.taskExecutor.setKeepAliveSeconds(60);
        this.taskExecutor.setAllowCoreThreadTimeOut(true);
        this.taskExecutor.initialize();

        this.taskScheduler = new ThreadPoolTaskScheduler();
        this.taskScheduler.setPoolSize(1);
        this.taskScheduler.setThreadNamePrefix("travel-burst-scheduler-");
        this.taskScheduler.initialize();
    }

    @PreDestroy
    public void cleanup() {
        LOGGER.info("[cleanup][Starting graceful shutdown]");
        
        // Shutdown task executor
        if (taskExecutor != null) {
            taskExecutor.shutdown();
            try {
                // Wait for existing tasks to terminate
                if (!taskExecutor.getThreadPoolExecutor().awaitTermination(30, TimeUnit.SECONDS)) {
                    LOGGER.warn("[cleanup][Tasks didn't complete in time, forcing shutdown]");
                    taskExecutor.getThreadPoolExecutor().shutdownNow();
                    
                    // Wait a while for tasks to respond to being cancelled
                    if (!taskExecutor.getThreadPoolExecutor().awaitTermination(10, TimeUnit.SECONDS)) {
                        LOGGER.error("[cleanup][Thread pool did not terminate]");
                    }
                }
            } catch (InterruptedException e) {
                // (Re-)Cancel if current thread also interrupted
                taskExecutor.getThreadPoolExecutor().shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
                LOGGER.error("[cleanup][Shutdown interrupted]", e);
            }
        }
        
        // Shutdown scheduler
        if (taskScheduler != null) {
            taskScheduler.shutdown();
            try {
                if (!taskScheduler.getScheduledThreadPoolExecutor().awaitTermination(10, TimeUnit.SECONDS)) {
                    LOGGER.warn("[cleanup][Scheduler didn't complete in time, forcing shutdown]");
                    taskScheduler.getScheduledThreadPoolExecutor().shutdownNow();
                }
            } catch (InterruptedException e) {
                taskScheduler.getScheduledThreadPoolExecutor().shutdownNow();
                Thread.currentThread().interrupt();
                LOGGER.error("[cleanup][Scheduler shutdown interrupted]", e);
            }
        }
        
        LOGGER.info("[cleanup][Graceful shutdown completed]");
    }

    private boolean shouldStartBurst() {
        long currentTime = Instant.now().getEpochSecond();
        long lastBurst = lastBurstTime.get();
        boolean toReturn = currentTime - lastBurst >= BURST_PERIOD_SECONDS;

        if (toReturn) {
            lastBurstTime.set(currentTime);
        }
        return toReturn;
    }

    private void makeSeatRequest(String url, HttpEntity<?> request, int burstId) {
        String currentTraceId = TraceContext.traceId();
        
        // Use existing headers or create new ones
        HttpHeaders headers = new HttpHeaders();
        if (request.getHeaders() != null) {
            headers.putAll(request.getHeaders());
        }
        
        // Ensure trace headers are set
        headers.set("sw8", currentTraceId);
        headers.set("sw8-correlation", currentTraceId);
        
        HttpEntity<?> requestWithTracing = new HttpEntity<>(
            request.getBody(),
            headers
        );

        LOGGER.info("[makeSeatRequest][Sending request][BurstID: {}][TraceID: {}]",
            burstId, currentTraceId);

        try {
            restTemplate.exchange(
                url,
                HttpMethod.POST,
                requestWithTracing,
                new ParameterizedTypeReference<Response<Integer>>() {}
            );
            LOGGER.info("[makeSeatRequest][Request completed][BurstID: {}][TraceID: {}]",
                burstId, currentTraceId);
        } catch (Exception e) {
            LOGGER.error("[makeSeatRequest][Request failed][BurstID: {}][TraceID: {}][Error: {}]",
                burstId, currentTraceId, e.getMessage());
            throw e;
        }
    }

    private void executeRestTicketBurst(String url, HttpEntity<?> request, SpanRef parentSpan) {
        final String rootTraceId = TraceContext.traceId();
        LOGGER.info("[burst][Starting burst requests][Root TraceID: {}]", rootTraceId);

        try {
            for (int i = 0; i < BURST_DURATION_SECONDS; i++) {
                final int burstGroup = i;
                long startTime = System.currentTimeMillis();
                CountDownLatch groupLatch = new CountDownLatch(BURST_REQUESTS_PER_SEC);

                for (int j = 0; j < BURST_REQUESTS_PER_SEC; j++) {
                    final int burstId = i * BURST_REQUESTS_PER_SEC + j + 1;
                    
                    // Create new headers with trace context for this request
                    HttpHeaders headers = new HttpHeaders();
                    if (request.getHeaders() != null) {
                        headers.putAll(request.getHeaders());
                    }
                    
                    // Add trace context to headers
                    headers.set("sw8", rootTraceId);
                    headers.set("sw8-correlation", rootTraceId);

                    // Create new request with trace context
                    final HttpEntity<?> requestWithContext = new HttpEntity<>(
                        request.getBody(),
                        headers
                    );
                    
                    taskExecutor.execute(() -> {
                        try {
                            LOGGER.info("[burst][Worker executing][BurstID: {}][TraceID: {}]", 
                                burstId, rootTraceId);

                            makeSeatRequest(url, requestWithContext, burstId);

                        } catch (Exception e) {
                            LOGGER.error("[burst][Worker failed][BurstID: {}][TraceID: {}][Error: {}]", 
                                burstId, rootTraceId, e.getMessage());
                        } finally {
                            groupLatch.countDown();
                        }
                    });
                }

                if (!groupLatch.await(1, TimeUnit.SECONDS)) {
                    LOGGER.warn("[burst][Group timeout][Group: {}][TraceID: {}]", 
                        burstGroup, rootTraceId);
                }

                long elapsedTime = System.currentTimeMillis() - startTime;
                if (elapsedTime < 1000) {
                    Thread.sleep(1000 - elapsedTime);
                }
            }
        } catch (Exception e) {
            LOGGER.error("[burst][Burst execution failed][TraceID: {}][Error: {}]", 
                rootTraceId, e.getMessage());
        }
    }


    @Override
    public Response create(TravelInfo info, HttpHeaders headers) {
        TripId ti = new TripId(info.getTripId());
        if (repository.findByTripId(ti) == null) {
            Trip trip = new Trip(ti, info.getTrainTypeName(), info.getStartStationName(),
                    info.getStationsName(), info.getTerminalStationName(), info.getStartTime(), info.getEndTime());
            trip.setRouteId(info.getRouteId());
            repository.save(trip);
            return new Response<>(1, "Create trip:" + ti.toString() + ".", null);
        } else {
            TravelServiceImpl.LOGGER.error("[create][Create trip error][Trip already exists][TripId: {}]", info.getTripId());
            return new Response<>(1, "Trip " + info.getTripId().toString() + " already exists", null);
        }
    }

    @Override
    public Response getRouteByTripId(String tripId, HttpHeaders headers) {
        Route route = null;
        if (null != tripId && tripId.length() >= 2) {
            TripId tripId1 = new TripId(tripId);
            Trip trip = repository.findByTripId(tripId1);
            if (trip != null) {
                route = getRouteByRouteId(trip.getRouteId(), headers);
            } else {
                TravelServiceImpl.LOGGER.error("[getRouteByTripId][Get route by Trip id error][Trip not found][TripId: {}]", tripId);
            }
        }
        if (route != null) {
            return new Response<>(1, success, route);
        } else {
            TravelServiceImpl.LOGGER.error("[getRouteByTripId][Get route by Trip id error][Route not found][TripId: {}]", tripId);
            return new Response<>(0, noContent, null);
        }
    }

    @Override
    public Response getTrainTypeByTripId(String tripId, HttpHeaders headers) {
        TripId tripId1 = new TripId(tripId);
        TrainType trainType = null;
        Trip trip = repository.findByTripId(tripId1);
        if (trip != null) {
            trainType = getTrainTypeByName(trip.getTrainTypeName(), headers);
        } else {
            TravelServiceImpl.LOGGER.error("[getTrainTypeByTripId][Get Train Type by Trip id error][Trip not found][TripId: {}]", tripId);
        }
        if (trainType != null) {
            return new Response<>(1, success, trainType);
        } else {
            TravelServiceImpl.LOGGER.error("[getTrainTypeByTripId][Get Train Type by Trip id error][Train Type not found][TripId: {}]", tripId);
            return new Response<>(0, noContent, null);
        }
    }

    @Override
    public Response getTripByRoute(ArrayList<String> routeIds, HttpHeaders headers) {
        ArrayList<ArrayList<Trip>> tripList = new ArrayList<>();
        for (String routeId : routeIds) {
            ArrayList<Trip> tempTripList = repository.findByRouteId(routeId);
            if (tempTripList == null) {
                tempTripList = new ArrayList<>();
            }
            tripList.add(tempTripList);
        }
        if (!tripList.isEmpty()) {
            return new Response<>(1, success, tripList);
        } else {
            TravelServiceImpl.LOGGER.warn("[getTripByRoute][Get trips by routes warn][Trip list][{}]", "No content");
            return new Response<>(0, noContent, null);
        }
    }


    @Override
    public Response retrieve(String tripId, HttpHeaders headers) {
        TripId ti = new TripId(tripId);
        Trip trip = repository.findByTripId(ti);
        if (trip != null) {
            return new Response<>(1, "Search Trip Success by Trip Id " + tripId, trip);
        } else {
            TravelServiceImpl.LOGGER.error("[retrieve][Retrieve trip error][Trip not found][TripId: {}]", tripId);
            return new Response<>(0, "No Content according to tripId" + tripId, null);
        }
    }

    @Override
    public Response update(TravelInfo info, HttpHeaders headers) {
        TripId ti = new TripId(info.getTripId());
        Trip t = repository.findByTripId(ti);
        if (t != null) {
            t.setStartStationName(info.getTrainTypeName());
            t.setStartStationName( info.getStartStationName());
            t.setStationsName(info.getStationsName());
            t.setTerminalStationName(info.getTerminalStationName());
            t.setStartTime(info.getStartTime());
            t.setEndTime(info.getEndTime());
            t.setRouteId(info.getRouteId());
            repository.save(t);
            return new Response<>(1, "Update trip:" + ti.toString(), t);
        } else {
            TravelServiceImpl.LOGGER.error("[update][Update trip error][Trip not found][TripId: {}]", info.getTripId());
            return new Response<>(1, "Trip" + info.getTripId().toString() + "doesn 't exists", null);
        }
    }

    @Override
    @Transactional
    public Response delete(String tripId, HttpHeaders headers) {
        TripId ti = new TripId(tripId);
        if (repository.findByTripId(ti) != null) {
            repository.deleteByTripId(ti);
            return new Response<>(1, "Delete trip:" + tripId + ".", tripId);
        } else {
            TravelServiceImpl.LOGGER.error("[delete][Delete trip error][Trip not found][TripId: {}]", tripId);
            return new Response<>(0, "Trip " + tripId + " doesn't exist.", null);
        }
    }

    @Override
    public Response query(TripInfo info, HttpHeaders headers) {

        //Gets the start and arrival stations of the train number to query. The originating and arriving stations received here are both station names, so two requests need to be sent to convert to station ids
        String startPlaceName = info.getStartPlace();
        String endPlaceName = info.getEndPlace();

        //This is the final result
        List<TripResponse> list = new ArrayList<>();

        //Check all train info
        List<Trip> allTripList = repository.findAll();
        if(allTripList != null) {
            for (Trip tempTrip : allTripList) {
                //Get the detailed route list of this train
                TripResponse response = getTickets(tempTrip, null, startPlaceName, endPlaceName, info.getDepartureTime(), headers);
                if (response == null) {
                    TravelServiceImpl.LOGGER.warn("[query][Query trip error][Tickets not found][start: {},end: {},time: {}]", startPlaceName, endPlaceName, info.getDepartureTime());
                }else{
                    list.add(response);
                }
            }
        }
        return new Response<>(1, success, list);
    }



    @Override
    public Response queryByBatch(TripInfo info, HttpHeaders headers) {

        //Gets the start and arrival stations of the train number to query. The originating and arriving stations received here are both station names, so two requests need to be sent to convert to station ids
        String startPlaceName = info.getStartPlace();
        String endPlaceName = info.getEndPlace();

        //This is the final result
        List<TripResponse> list = new ArrayList<>();

        //Check all train info
        List<Trip> allTripList = repository.findAll();
        list = getTicketsByBatch(allTripList, startPlaceName, endPlaceName, info.getDepartureTime(), headers);
        return new Response<>(1, success, list);
    }

    @TraceCrossThread
    class MyCallable implements Callable<TripResponse> {
        private TripInfo info;
        private Trip tempTrip;
        private HttpHeaders headers;
        private String startPlaceName;
        private String endPlaceName;

        MyCallable(TripInfo info, String startPlaceName, String endPlaceName, Trip tempTrip, HttpHeaders headers) {
            this.info = info;
            this.tempTrip = tempTrip;
            this.headers = headers;
            this.startPlaceName = startPlaceName;
            this.endPlaceName = endPlaceName;
        }

        @Override
        public TripResponse call() throws Exception {
            TravelServiceImpl.LOGGER.debug("[call][Start to query][tripId: {}, routeId: {}] ", tempTrip.getTripId().toString(), tempTrip.getRouteId());

            String startPlaceName = info.getStartPlace();
            String endPlaceName = info.getEndPlace();
            //Route tempRoute = getRouteByRouteId(tempTrip.getRouteId(), headers);

            TripResponse response = null;

            response = getTickets(tempTrip, null, startPlaceName, endPlaceName, info.getDepartureTime(), headers);

            if (response == null) {
                TravelServiceImpl.LOGGER.warn("[call][Query trip error][Tickets not found][tripId: {}, routeId: {}, start: {}, end: {},time: {}]", tempTrip.getTripId().toString(), tempTrip.getRouteId(), startPlaceName, endPlaceName, info.getDepartureTime());
            } else {
                TravelServiceImpl.LOGGER.info("[call][Query trip success][tripId: {}, routeId: {}] ", tempTrip.getTripId().toString(), tempTrip.getRouteId());
            }
            return response;
        }
    }

    @Override
    public Response queryInParallel(TripInfo info, HttpHeaders headers) {
        //Gets the start and arrival stations of the train number to query. The originating and arriving stations received here are both station names, so two requests need to be sent to convert to station ids
        String startPlaceName = info.getStartPlace();
        String endPlaceName = info.getEndPlace();

        //This is the final result
        List<TripResponse> list = new ArrayList<>();

        //Check all train info
        List<Trip> allTripList = repository.findAll();
        List<Future<TripResponse>> futureList = new ArrayList<>();

        if(allTripList != null ){
            for (Trip tempTrip : allTripList) {
                MyCallable callable = new MyCallable(info, startPlaceName, endPlaceName, tempTrip, headers);
                Future<TripResponse> future = executorService.submit(callable);
                futureList.add(future);
            }
        }

        for (Future<TripResponse> future : futureList) {
            try {
                TripResponse response = future.get();
                if (response != null) {
                    list.add(response);
                }
            } catch (Exception e) {
                TravelServiceImpl.LOGGER.error("[queryInParallel][Query error]"+e.toString());
            }
        }

        if (list.isEmpty()) {
            return new Response<>(0, "No Trip info content", null);
        } else {
            return new Response<>(1, success, list);
        }
    }

    @Override
    public Response getTripAllDetailInfo(TripAllDetailInfo gtdi, HttpHeaders headers) {
        TripAllDetail gtdr = new TripAllDetail();
        TravelServiceImpl.LOGGER.debug("[getTripAllDetailInfo][TripId: {}]", gtdi.getTripId());
        Trip trip = repository.findByTripId(new TripId(gtdi.getTripId()));
        if (trip == null) {
            gtdr.setTripResponse(null);
            gtdr.setTrip(null);
            TravelServiceImpl.LOGGER.error("[getTripAllDetailInfo][Get trip detail error][Trip not found][TripId: {}]", gtdi.getTripId());
            return new Response<>(0, "Trip not found", gtdr);
        } else {
            String startPlaceName = gtdi.getFrom();
            String endPlaceName = gtdi.getTo();
            String travelDate = gtdi.getTravelDate();
            
            // Create a copy of the trip with updated travel date
            Trip updatedTrip = createTripWithNewDate(trip, travelDate);
            
            TripResponse tripResponse = getTickets(updatedTrip, null, startPlaceName, endPlaceName, travelDate, headers);
            if (tripResponse == null) {
                gtdr.setTripResponse(null);
                gtdr.setTrip(null);
                TravelServiceImpl.LOGGER.warn("[getTripAllDetailInfo][Get trip detail error][Tickets not found][start: {},end: {},time: {}]", startPlaceName, endPlaceName, travelDate);
                return new Response<>(0, "getTickets failed", gtdr);
            } else {
                gtdr.setTripResponse(tripResponse);
                gtdr.setTrip(updatedTrip);
            }
        }
        return new Response<>(1, success, gtdr);
    }

    private Trip createTripWithNewDate(Trip originalTrip, String newDate) {
        Trip updatedTrip = new Trip(
            originalTrip.getTripId(),
            originalTrip.getTrainTypeName(),
            originalTrip.getStartStationName(),
            originalTrip.getStationsName(),
            originalTrip.getTerminalStationName(),
            updateDateOnly(originalTrip.getStartTime(), newDate),
            updateDateOnly(originalTrip.getEndTime(), newDate)
        );
        updatedTrip.setRouteId(originalTrip.getRouteId());
        return updatedTrip;
    }
    
    private String updateDateOnly(String originalDateTime, String newDate) {
        // Assuming the date format is "yyyy-MM-dd HH:mm:ss"
        String[] dateTimeParts = originalDateTime.split(" ");
        return newDate + " " + dateTimeParts[1];
    }

    private List<TripResponse> getTicketsByBatch(List<Trip> trips, String startPlaceName, String endPlaceName, String departureTime, HttpHeaders headers) {
        List<TripResponse> responses = new ArrayList<>();
        //Determine if the date checked is the same day and after
        if (!afterToday(departureTime)) {
            TravelServiceImpl.LOGGER.info("[getTickets][depaturetime not vailid][departuretime: {}]", departureTime);
            return responses;
        }

        List<Travel> infos = new ArrayList<>();
        Map<String, Trip> tripMap = new HashMap<>();
        for(Trip trip: trips){
            Travel query = new Travel();
            query.setTrip(trip);
            query.setStartPlace(startPlaceName);
            query.setEndPlace(endPlaceName);
            query.setDepartureTime(departureTime);

            infos.add(query);
            tripMap.put(trip.getTripId().toString(), trip);
        }

        TravelServiceImpl.LOGGER.info("[getTicketsByBatch][before get basic][trips: {}]", trips);

        HttpEntity requestEntity = new HttpEntity(infos, null);
        String basic_service_url = getServiceUrl("ts-basic-service");
        ResponseEntity<Response> re = restTemplate.exchange(
                basic_service_url + "/api/v1/basicservice/basic/travels",
                HttpMethod.POST,
                requestEntity,
                Response.class);

        Response r = re.getBody();
        if(r.getStatus() == 0){
            TravelServiceImpl.LOGGER.info("[getTicketsByBatch][Ts-basic-service response status is 0][response is: {}]", r);
            return responses;
        }
        Map<String, TravelResult> trMap;
        ObjectMapper mapper = new ObjectMapper();
        try{
            trMap = mapper.readValue(JsonUtils.object2Json(r.getData()), new TypeReference<Map<String, TravelResult>>(){});
        }catch(Exception e) {
            TravelServiceImpl.LOGGER.warn("[getTicketsByBatch][Ts-basic-service convert data failed][Fail msg: {}]", e.getMessage());
            return responses;
        }

        for(Map.Entry<String, TravelResult> trEntry: trMap.entrySet()){
            //Set the returned ticket information
            String tripNumber = trEntry.getKey();
            TravelResult tr = trEntry.getValue();
            Trip trip = tripMap.get(tripNumber);

            TripResponse response = setResponse(trip, tr, startPlaceName, endPlaceName, departureTime, headers);
            responses.add(response);
        }
        return responses;
    }

    private TripResponse getTickets(Trip trip, Route route1, String startPlaceName, String endPlaceName, String departureTime, HttpHeaders headers) {

        //Determine if the date checked is the same day and after
        if (!afterToday(departureTime)) {
            TravelServiceImpl.LOGGER.info("[getTickets][depaturetime not vailid][departuretime: {}]", departureTime);
            return null;
        }

        Travel query = new Travel();
        query.setTrip(trip);
        query.setStartPlace(startPlaceName);
        query.setEndPlace(endPlaceName);
        query.setDepartureTime(departureTime);
        TravelServiceImpl.LOGGER.info("[getTickets][before get basic][trip: {}]", trip);

        HttpEntity requestEntity = new HttpEntity(query, null);
        String basic_service_url = getServiceUrl("ts-basic-service");
        ResponseEntity<Response> re = restTemplate.exchange(
                basic_service_url + "/api/v1/basicservice/basic/travel",
                HttpMethod.POST,
                requestEntity,
                Response.class);

        Response r = re.getBody();
        if(r.getStatus() == 0){
            TravelServiceImpl.LOGGER.info("[getTickets][Ts-basic-service response status is 0][response is: {}]", r);
            return null;
        }

        TravelResult resultForTravel = JsonUtils.conveterObject(re.getBody().getData(), TravelResult.class);

        //Set the returned ticket information
        return setResponse(trip, resultForTravel, startPlaceName, endPlaceName, departureTime, headers);
    }

    private TripResponse setResponse(Trip trip, TravelResult tr, String startPlaceName, String endPlaceName, String departureTime, HttpHeaders headers){
        //Set the returned ticket information
        TripResponse response = new TripResponse();
        response.setConfortClass(50);
        response.setEconomyClass(50);

        Route route = tr.getRoute();
        List<String> stationList = route.getStations();

        int firstClassTotalNum = tr.getTrainType().getConfortClass();
        int secondClassTotalNum = tr.getTrainType().getEconomyClass();

        int first = getRestTicketNumber(departureTime, trip.getTripId().toString(),
                startPlaceName, endPlaceName, SeatClass.FIRSTCLASS.getCode(), firstClassTotalNum, stationList, headers);

        int second = getRestTicketNumber(departureTime, trip.getTripId().toString(),
                startPlaceName, endPlaceName, SeatClass.SECONDCLASS.getCode(), secondClassTotalNum, stationList, headers);
        response.setConfortClass(first);
        response.setEconomyClass(second);

        response.setStartStation(startPlaceName);
        response.setTerminalStation(endPlaceName);

        //Calculate the distance from the starting point
        int indexStart = route.getStations().indexOf(startPlaceName);
        int indexEnd = route.getStations().indexOf(endPlaceName);
        int distanceStart = route.getDistances().get(indexStart) - route.getDistances().get(0);
        int distanceEnd = route.getDistances().get(indexEnd) - route.getDistances().get(0);
        TrainType trainType = tr.getTrainType();
        //Train running time is calculated according to the average running speed of the train
        int minutesStart = 60 * distanceStart / trainType.getAverageSpeed();
        int minutesEnd = 60 * distanceEnd / trainType.getAverageSpeed();

        Calendar calendarStart = Calendar.getInstance();
        calendarStart.setTime(StringUtils.String2Date(departureTime + " " + trip.getStartTime().split(" ")[1]));
        calendarStart.add(Calendar.MINUTE, minutesStart);
        response.setStartTime(StringUtils.Date2String(calendarStart.getTime()));
        TravelServiceImpl.LOGGER.info("[getTickets][Calculate distance][calculate time：{}  time: {}]", minutesStart, calendarStart.getTime());

        Calendar calendarEnd = Calendar.getInstance();
        calendarEnd.setTime(StringUtils.String2Date(departureTime + " " + trip.getStartTime().split(" ")[1]));
        calendarEnd.add(Calendar.MINUTE, minutesEnd);
        response.setEndTime(StringUtils.Date2String(calendarEnd.getTime()));
        TravelServiceImpl.LOGGER.info("[getTickets][Calculate distance][calculate time：{}  time: {}]", minutesEnd, calendarEnd.getTime());

        response.setTripId(trip.getTripId());
        response.setTrainTypeName(trip.getTrainTypeName());
        response.setPriceForConfortClass(tr.getPrices().get("confortClass"));
        response.setPriceForEconomyClass(tr.getPrices().get("economyClass"));

        return response;
    }

    @Override
    public Response queryAll(HttpHeaders headers) {
        List<Trip> tripList = repository.findAll();
        if (tripList != null && !tripList.isEmpty()) {
            TravelServiceImpl.LOGGER.info("[queryAll][Query all trips:][{}]", "tripList");
            return new Response<>(1, success, tripList);
        }
        TravelServiceImpl.LOGGER.warn("[queryAll][Query all trips warn][{}]", "No Content");
        return new Response<>(0, noContent, null);
    }

    private static boolean afterToday(String date) {
        Calendar calDateA = Calendar.getInstance();
        Date today = new Date();
        calDateA.setTime(today);

        Calendar calDateB = Calendar.getInstance();
        calDateB.setTime(StringUtils.String2Date(date));

        // TravelServiceImpl.LOGGER.info("[today date][y: {}][m:{}][d: {}]",calDateA.get(Calendar.YEAR), calDateA.get(Calendar.MONTH), calDateA.get(Calendar.DATE));
        // TravelServiceImpl.LOGGER.info("[departrue date][y: {}][m:{}][d: {}]",calDateB.get(Calendar.YEAR), calDateB.get(Calendar.MONTH), calDateB.get(Calendar.DATE));

        TravelServiceImpl.LOGGER.info("[afterToday][Today date][y: {}][m:{}][d: {}]",
            calDateA.get(Calendar.YEAR), calDateA.get(Calendar.MONTH), calDateA.get(Calendar.DAY_OF_MONTH));
        TravelServiceImpl.LOGGER.info("[afterToday][Departure date][y: {}][m:{}][d: {}]",
            calDateB.get(Calendar.YEAR), calDateB.get(Calendar.MONTH), calDateB.get(Calendar.DAY_OF_MONTH));

        return !calDateB.before(calDateA);
    }

    private TrainType getTrainTypeByName(String trainTypeName, HttpHeaders headers) {
        HttpEntity requestEntity = new HttpEntity(null);
        String train_service_url = getServiceUrl("ts-train-service");
        ResponseEntity<Response<TrainType>> re = restTemplate.exchange(
                train_service_url + "/api/v1/trainservice/trains/byName/" + trainTypeName,
                HttpMethod.GET,
                requestEntity,
                new ParameterizedTypeReference<Response<TrainType>>() {
                });

        return re.getBody().getData();
    }

    private Route getRouteByRouteId(String routeId, HttpHeaders headers) {
        TravelServiceImpl.LOGGER.info("[getRouteByRouteId][Get Route By Id][Route ID：{}]", routeId);
        HttpEntity requestEntity = new HttpEntity(null);
        String route_service_url = getServiceUrl("ts-route-service");
        ResponseEntity<Response> re = restTemplate.exchange(
                route_service_url + "/api/v1/routeservice/routes/" + routeId,
                HttpMethod.GET,
                requestEntity,
                Response.class);
        Response routeRes = re.getBody();

        Route route1 = new Route();
        TravelServiceImpl.LOGGER.info("[getRouteByRouteId][Get Route By Id][Routes Response is : {}]", routeRes.toString());
        if (routeRes.getStatus() == 1) {
            route1 = JsonUtils.conveterObject(routeRes.getData(), Route.class);
            TravelServiceImpl.LOGGER.info("[getRouteByRouteId][Get Route By Id][Route is: {}]", route1.toString());
        }
        return route1;
    }


    private int getRestTicketNumber(String travelDate, String trainNumber, String startStationName, String endStationName, int seatType, int totalNum, List<String> stationList, HttpHeaders headers) {
            
        String parentTraceId = TraceContext.traceId();
        LOGGER.info("[getRestTicketNumber][Start query][TraceId: {}]", parentTraceId);

        SpanRef rootSpan = null;
        try {
            // Create entry span for the main operation
            rootSpan = Tracer.createEntrySpan("get.rest.ticket", null);
            rootSpan.tag("train.number", trainNumber);
            rootSpan.tag("parent.traceId", parentTraceId);

            // Create local span for seat request preparation
            SpanRef prepareSpan = Tracer.createLocalSpan("prepare.seat.request");
            prepareSpan.tag("parent.traceId", parentTraceId);
            
            Seat seatRequest = new Seat();
            seatRequest.setDestStation(endStationName);
            seatRequest.setStartStation(startStationName);
            seatRequest.setTrainNumber(trainNumber);
            seatRequest.setTravelDate(travelDate);
            seatRequest.setSeatType(seatType);
            seatRequest.setTotalNum(totalNum);
            seatRequest.setStations(stationList);

            HttpHeaders requestHeaders = new HttpHeaders();
            if (headers != null) {
                requestHeaders.putAll(headers);
            }
            requestHeaders.set("sw8", parentTraceId);
            HttpEntity<?> requestEntity = new HttpEntity<>(seatRequest, requestHeaders);
            
            Tracer.stopSpan(); // Stop prepare span

            // Create exit span for seat service call
            String url = getServiceUrl("ts-seat-service") + "/api/v1/seatservice/seats/left_tickets";
            SpanRef exitSpan = Tracer.createExitSpan("query.seat.tickets", "ts-seat-service");
            exitSpan.tag("seat.type", String.valueOf(seatType));

            ResponseEntity<Response<Integer>> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                requestEntity,
                new ParameterizedTypeReference<Response<Integer>>() {}
            );
            
            Tracer.stopSpan(); // Stop exit span

            // Handle burst requests if needed
            if (response.getBody() != null && response.getBody().getStatus() == 1 
                && shouldStartBurst()) {
                SpanRef burstSpan = Tracer.createLocalSpan("init.burst.requests");
                burstSpan.tag("burst.count", String.valueOf(BURST_REQUESTS_PER_SEC * BURST_DURATION_SECONDS));
                burstSpan.prepareForAsync(); // Keep span alive for async operations

                LOGGER.info("[getRestTicketNumber][Starting burst requests][TraceId: {}]", parentTraceId);
                executeRestTicketBurst(url, requestEntity, burstSpan);
                
                Tracer.stopSpan(); // Stop burst span
            }

            if (response.getBody() != null) {
                LOGGER.debug("[getRestTicketNumber][Query success][TraceId: {}][Result: {}]", 
                    parentTraceId, response.getBody().getData());
                return response.getBody().getData();
            }
            return 0;

        } catch (Exception e) {
            if (rootSpan != null) {
                rootSpan.log(e);
                rootSpan.tag("error", "true");
                rootSpan.tag("error.message", e.getMessage());
            }
            LOGGER.error("[getRestTicketNumber][Query failed][TraceId: {}][Error: {}]", 
                parentTraceId, e.getMessage());
            return 0;
        } finally {
            if (rootSpan != null) {
                Tracer.stopSpan();
            }
        }
    }

    @Override
    public Response adminQueryAll(HttpHeaders headers) {
        List<Trip> trips = repository.findAll();
        ArrayList<AdminTrip> adminTrips = new ArrayList<>();
        if(trips != null){
            for (Trip trip : trips) {
                AdminTrip adminTrip = new AdminTrip();
                adminTrip.setTrip(trip);
                adminTrip.setRoute(getRouteByRouteId(trip.getRouteId(), headers));
                adminTrip.setTrainType(getTrainTypeByName(trip.getTrainTypeName(), headers));
                adminTrips.add(adminTrip);
            }
        }

        if (!adminTrips.isEmpty()) {
            return new Response<>(1, success, adminTrips);
        } else {
            TravelServiceImpl.LOGGER.warn("[adminQueryAll][Admin query all trips warn][{}]", "No Content");
            return new Response<>(0, noContent, null);
        }
    }
}
