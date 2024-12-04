package seat.service;

import edu.fudan.common.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import edu.fudan.common.entity.*;

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
public class SeatServiceImpl implements SeatService {

    private static final int BURST_REQUESTS_PER_SEC = 10;
    private static final int BURST_DURATION_SECONDS = 10;
    private static final int BURST_PERIOD_SECONDS = 60;
    private static final int THREAD_POOL_SIZE = Math.max(1, BURST_REQUESTS_PER_SEC * 2);
    
    // Executors for burst handling
    private ThreadPoolTaskExecutor taskExecutor;
    private ThreadPoolTaskScheduler taskScheduler;
    private static final AtomicLong lastBurstTime = new AtomicLong(0);

    @Autowired
    private TaskDecorator traceContextDecorator;

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    private DiscoveryClient discoveryClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(SeatServiceImpl.class);

    private String getServiceUrl(String serviceName) {
        return "http://" + serviceName;
    }

    @PostConstruct
    public void init() {
        this.taskExecutor = new ThreadPoolTaskExecutor();
        this.taskExecutor.setCorePoolSize(BURST_REQUESTS_PER_SEC);
        this.taskExecutor.setMaxPoolSize(THREAD_POOL_SIZE);
        this.taskExecutor.setQueueCapacity(100);
        this.taskExecutor.setThreadNamePrefix("seat-burst-worker-");
        this.taskExecutor.setTaskDecorator(traceContextDecorator);
        this.taskExecutor.initialize();

        this.taskScheduler = new ThreadPoolTaskScheduler();
        this.taskScheduler.setPoolSize(1);
        this.taskScheduler.setThreadNamePrefix("seat-burst-scheduler-");
        this.taskScheduler.initialize();
    }

    @PreDestroy
    public void cleanup() {
        if (taskExecutor != null) {
            taskExecutor.shutdown();
        }
        if (taskScheduler != null) {
            taskScheduler.shutdown();
        }
    }

    private boolean shouldStartBurst() {
        long currentTime = Instant.now().getEpochSecond();
        long lastBurst = lastBurstTime.get();
        return currentTime - lastBurst >= BURST_PERIOD_SECONDS && 
            lastBurstTime.compareAndSet(lastBurst, currentTime);
    }

    private void makeOrderRequest(String url, HttpEntity<?> request) {
        ResponseEntity<Response<LeftTicketInfo>> response = restTemplate.exchange(
            url,
            HttpMethod.POST,
            request,
            new ParameterizedTypeReference<Response<LeftTicketInfo>>() {}
        );
    }

    // Helper method to process the original seat distribution logic
    private Response processDistributeSeat(Seat seatRequest, HttpHeaders headers) {
        // Original distributeSeat logic

        LeftTicketInfo leftTicketInfo;
        ResponseEntity<Response<LeftTicketInfo>> re3;

        //Distinguish G\D from other trains
        String trainNumber = seatRequest.getTrainNumber();

        if (trainNumber.startsWith("G") || trainNumber.startsWith("D")) {
            LOGGER.info("[distributeSeat][TrainNumber start][G or D]");

            HttpEntity<?> requestEntity = new HttpEntity<>(seatRequest, headers);
            String order_service_url = getServiceUrl("ts-order-service");
            re3 = restTemplate.exchange(
                order_service_url + "/api/v1/orderservice/order/tickets",
                HttpMethod.POST,
                requestEntity,
                new ParameterizedTypeReference<Response<LeftTicketInfo>>() {}
            );
            
            LOGGER.info("[distributeSeat][Left ticket info][info is : {}]", re3.getBody().toString());
            leftTicketInfo = re3.getBody().getData();
        } else {
            SeatServiceImpl.LOGGER.info("[distributeSeat][TrainNumber start][Other Capital Except D and G]");
            //Call the microservice to query for residual Ticket information: the set of the Ticket sold for the specified seat type
            HttpEntity requestEntity = new HttpEntity(seatRequest, null);
            String order_other_service_url=getServiceUrl("ts-order-other-service");
            re3 = restTemplate.exchange(
                    order_other_service_url + "/api/v1/orderOtherService/orderOther/tickets",
                    HttpMethod.POST,
                    requestEntity,
                    new ParameterizedTypeReference<Response<LeftTicketInfo>>() {
                    });
            SeatServiceImpl.LOGGER.info("[distributeSeat][Left ticket info][info is : {}]", re3.getBody().toString());
            leftTicketInfo = re3.getBody().getData();
        }

        //Assign seats
        List<String> stationList = seatRequest.getStations();

        int seatTotalNum = seatRequest.getTotalNum();
        String startStation = seatRequest.getStartStation();
        Ticket ticket = new Ticket();
        ticket.setStartStation(startStation);
        ticket.setDestStation(seatRequest.getDestStation());

        //Assign new tickets
        Random rand = new Random();
        int range = seatTotalNum;
        int seat = rand.nextInt(range) + 1;

        if(leftTicketInfo != null) {
            Set<Ticket> soldTickets = leftTicketInfo.getSoldTickets();
            //Give priority to tickets already sold
            for (Ticket soldTicket : soldTickets) {
                String soldTicketDestStation = soldTicket.getDestStation();
                //Tickets can be allocated if the sold ticket's end station before the start station of the request
                if (stationList.indexOf(soldTicketDestStation) < stationList.indexOf(startStation)) {
                    ticket.setSeatNo(soldTicket.getSeatNo());
                    SeatServiceImpl.LOGGER.info("[distributeSeat][Assign new tickets][Use the previous distributed seat number][seat number:{}]", soldTicket.getSeatNo());
                    return new Response<>(1, "Use the previous distributed seat number!", ticket);
                }
            }
            while (isContained(soldTickets, seat)) {
                seat = rand.nextInt(range) + 1;
            }
        }
        ticket.setSeatNo(seat);
        SeatServiceImpl.LOGGER.info("[distributeSeat][Assign new tickets][Use a new seat number][seat number:{}]", seat);
        return new Response<>(1, "Use a new seat number!", ticket);
    }


    @Override
    public Response distributeSeat(Seat seatRequest, HttpHeaders headers) {
        String traceId = TraceContext.traceId();
        String segmentId = TraceContext.segmentId();
        String parentTraceId = headers.getFirst("sw8");

        LOGGER.info("[seat][Received request][TraceID: {}][SegmentID: {}][Parent TraceID: {}]",
            traceId, segmentId, parentTraceId);

        try {
            // Create span for seat processing
            SpanRef seatSpan = Tracer.createLocalSpan("seat.process");
            seatSpan.tag("parent.traceId", parentTraceId);

            // Create carrier for downstream calls
            ContextCarrierRef carrier = new ContextCarrierRef();
            Tracer.inject(carrier);

            // Prepare headers for order service call
            HttpHeaders orderHeaders = new HttpHeaders();
            if (headers != null) {
                orderHeaders.putAll(headers);
            }

            CarrierItemRef item = carrier.items();
            while (item.hasNext()) {
                item = item.next();
                orderHeaders.set(item.getHeadKey(), item.getHeadValue());
            }

            // Ensure SW8 correlation
            orderHeaders.set("sw8", traceId);

            // Create request for order service
            HttpEntity<?> orderRequest = new HttpEntity<>(seatRequest, orderHeaders);

            // Make order service call with propagated context
            String orderServiceUrl = getServiceUrl("ts-order-service");
            ResponseEntity<Response<LeftTicketInfo>> response;
            
            SpanRef orderSpan = null;
            try {
                orderSpan = Tracer.createExitSpan("query.order", "ts-order-service");
                response = restTemplate.exchange(
                    orderServiceUrl + "/api/v1/orderservice/order/tickets",
                    HttpMethod.POST,  
                    orderRequest,
                    new ParameterizedTypeReference<Response<LeftTicketInfo>>() {}
                );
            } finally {
                if (orderSpan != null) {
                    Tracer.stopSpan();
                }
            }

            // Process response and continue with seat allocation
            Response seatResponse = processDistributeSeat(seatRequest, orderHeaders);
            
            LOGGER.info("[seat][Request completed][TraceID: {}]", traceId);
            return seatResponse;

        } catch (Exception e) {
            LOGGER.error("[seat][Request failed][TraceID: {}][Error: {}]", traceId, e.getMessage());
            throw e;
        } finally {
            if (seatSpan != null) {
                Tracer.stopSpan();
            }
        }
    }

    private boolean isContained(Set<Ticket> soldTickets, int seat) {
        //Check that the seat number has been used
        boolean result = false;
        for (Ticket soldTicket : soldTickets) {
            if (soldTicket.getSeatNo() == seat) {
                return true;
            }
        }
        return result;
    }

    @Override
    public Response getLeftTicketOfInterval(Seat seatRequest, HttpHeaders headers) {
        int numOfLeftTicket = 0;
        LeftTicketInfo leftTicketInfo;
        ResponseEntity<Response<LeftTicketInfo>> re3;

        //Distinguish G\D from other trains
        String trainNumber = seatRequest.getTrainNumber();
        SeatServiceImpl.LOGGER.info("[getLeftTicketOfInterval][Seat request][request:{}]", seatRequest.toString());
        if (trainNumber.startsWith("G") || trainNumber.startsWith("D")) {
            SeatServiceImpl.LOGGER.info("[getLeftTicketOfInterval][TrainNumber start with G|D][trainNumber:{}]", trainNumber);

            //Call the micro service to query all the station information for the trains
            HttpEntity requestEntity = new HttpEntity(seatRequest, null);
            String order_service_url=getServiceUrl("ts-order-service");
            re3 = restTemplate.exchange(
                    order_service_url + "/api/v1/orderservice/order/tickets",
                    HttpMethod.POST,
                    requestEntity,
                    new ParameterizedTypeReference<Response<LeftTicketInfo>>() {
                    });

            SeatServiceImpl.LOGGER.info("[getLeftTicketOfInterval][Get Order tickets result][result is {}]", re3);
            leftTicketInfo = re3.getBody().getData();
        } else {
            SeatServiceImpl.LOGGER.info("[getLeftTicketOfInterval][TrainNumber start with other capital][trainNumber:{}]", trainNumber);
            //Call the micro service to query all the station information for the trains
            HttpEntity requestEntity = new HttpEntity(null);
            //Call the micro service to query for residual Ticket information: the set of the Ticket sold for the specified seat type
            requestEntity = new HttpEntity(seatRequest, null);
            String order_other_service_url=getServiceUrl("ts-order-other-service");
            re3 = restTemplate.exchange(
                    order_other_service_url + "/api/v1/orderOtherService/orderOther/tickets",
                    HttpMethod.POST,
                    requestEntity,
                    new ParameterizedTypeReference<Response<LeftTicketInfo>>() {
                    });
            SeatServiceImpl.LOGGER.info("[getLeftTicketOfInterval][Get Order tickets result][result is {}]", re3);
            leftTicketInfo = re3.getBody().getData();
        }

        //Counting the seats remaining in certain sections
        List<String> stationList = seatRequest.getStations();
        int seatTotalNum = seatRequest.getTotalNum();
        int solidTicketSize = 0;
        if (leftTicketInfo != null) {
            String startStation = seatRequest.getStartStation();
            Set<Ticket> soldTickets = leftTicketInfo.getSoldTickets();
            solidTicketSize = soldTickets.size();
            //To find out if tickets already sold are available
            for (Ticket soldTicket : soldTickets) {
                String soldTicketDestStation = soldTicket.getDestStation();
                //Tickets can be allocated if the sold ticket's end station before the start station of the request
                if (stationList.indexOf(soldTicketDestStation) < stationList.indexOf(startStation)) {
                    SeatServiceImpl.LOGGER.info("[getLeftTicketOfInterval][Ticket available or sold][The previous distributed seat number is usable][{}]", soldTicket.getSeatNo());
                    numOfLeftTicket++;
                }
            }
        }
        //Count the unsold tickets

        double direstPart = getDirectProportion(headers);

        if (stationList.get(0).equals(seatRequest.getStartStation()) &&
                stationList.get(stationList.size() - 1).equals(seatRequest.getDestStation())) {
            //do nothing
        } else {
            direstPart = 1.0 - direstPart;
        }

        int unusedNum = (int) (seatTotalNum * direstPart) - solidTicketSize;
        numOfLeftTicket += unusedNum;

        return new Response<>(1, "Get Left Ticket of Internal Success", numOfLeftTicket);
    }

    private double getDirectProportion(HttpHeaders headers) {

        String configName = "DirectTicketAllocationProportion";
        HttpEntity requestEntity = new HttpEntity(null);
        String config_service_url = getServiceUrl("ts-config-service");
        ResponseEntity<Response<Config>> re = restTemplate.exchange(
                config_service_url + "/api/v1/configservice/configs/" + configName,
                HttpMethod.GET,
                requestEntity,
                new ParameterizedTypeReference<Response<Config>>() {
                });
        Response<Config> configValue = re.getBody();
        SeatServiceImpl.LOGGER.info("[getDirectProportion][Configs is : {}]", configValue.getData().toString());
        return Double.parseDouble(configValue.getData().getValue());
    }
}