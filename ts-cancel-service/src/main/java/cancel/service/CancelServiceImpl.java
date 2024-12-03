package cancel.service;

import edu.fudan.common.entity.NotifyInfo;
import edu.fudan.common.entity.OrderStatus;
import edu.fudan.common.entity.Order;
import edu.fudan.common.entity.SeatClass;
import edu.fudan.common.entity.User;
import edu.fudan.common.util.Response;
import edu.fudan.common.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.apache.skywalking.apm.toolkit.trace.*;
import org.apache.skywalking.apm.toolkit.trace.ActiveSpan;
import org.apache.skywalking.apm.toolkit.trace.CallableWrapper;
import org.apache.skywalking.apm.toolkit.trace.RunnableWrapper;
import org.apache.skywalking.apm.toolkit.trace.TraceContext;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.Date;
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
public class CancelServiceImpl implements CancelService {

    private static final int BURST_REQUESTS_PER_SEC = 10;
    private static final int BURST_DURATION_SECONDS = 10;
    private static final int BURST_PERIOD_SECONDS = 60;
    private static final int THREAD_POOL_SIZE = Math.max(1, BURST_REQUESTS_PER_SEC * 2);
    
    private ThreadPoolTaskExecutor taskExecutor;
    private ThreadPoolTaskScheduler taskScheduler;
    private static final AtomicLong lastBurstTime = new AtomicLong(0);

    @Autowired
    private TaskDecorator traceContextDecorator;

    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private DiscoveryClient discoveryClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(CancelServiceImpl.class);

    String orderStatusCancelNotPermitted = "Order Status Cancel Not Permitted";

    private String getServiceUrl(String serviceName) {
        return "http://" + serviceName;
    }

    @PostConstruct
    public void init() {
        this.taskExecutor = new ThreadPoolTaskExecutor();
        this.taskExecutor.setCorePoolSize(BURST_REQUESTS_PER_SEC);
        this.taskExecutor.setMaxPoolSize(THREAD_POOL_SIZE);
        this.taskExecutor.setQueueCapacity(100);
        this.taskExecutor.setThreadNamePrefix("cancel-burst-worker-");
        this.taskExecutor.setTaskDecorator(traceContextDecorator);
        this.taskExecutor.initialize();

        this.taskScheduler = new ThreadPoolTaskScheduler();
        this.taskScheduler.setPoolSize(1);
        this.taskScheduler.setThreadNamePrefix("cancel-burst-scheduler-");
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

    private void makeCancelRequest(String url, HttpEntity<?> request) {
        ResponseEntity<Response> response = restTemplate.exchange(
            url,
            HttpMethod.POST,
            request,
            Response.class
        );
    }

    @Override
    public Response cancelOrder(String orderId, String loginId, HttpHeaders headers) {
        String traceId = org.apache.skywalking.apm.toolkit.trace.TraceContext.traceId();
        LOGGER.info("[cancelOrder][Cancel Order][OrderId: {}][TraceId: {}]", orderId, traceId);

        try {
            Response<Order> orderResult = getOrderByIdFromOrder(orderId, headers);
            Response response = null;

            if (orderResult.getStatus() == 1) {
                response = handleOrderCancellation(orderResult.getData(), loginId, headers);
            } else {
                Response<Order> orderOtherResult = getOrderByIdFromOrderOther(orderId, headers);
                if (orderOtherResult.getStatus() == 1) {
                    response = handleOrderCancellation(orderOtherResult.getData(), loginId, headers);
                } else {
                    return new Response<>(0, "Order Not Found.", null);
                }
            }

            // If we successfully cancelled the order, check if we should do burst requests
            if (response != null && response.getStatus() == 1 && shouldStartBurst()) {
                LOGGER.info("[cancelOrder][Starting burst requests][TraceId: {}]", traceId);
                
                String cancelUrl = getServiceUrl("ts-cancel-service") + "/api/v1/cancelservice/cancel/" + orderId + "/" + loginId;
                HttpEntity<?> requestEntity = new HttpEntity<>(null, headers);

                for (int i = 0; i < BURST_DURATION_SECONDS; i++) {
                    CountDownLatch latch = new CountDownLatch(BURST_REQUESTS_PER_SEC);
                    
                    for (int j = 0; j < BURST_REQUESTS_PER_SEC; j++) {
                        final int burstId = i * BURST_REQUESTS_PER_SEC + j + 1;
                        taskExecutor.execute(() -> {
                            try {
                                makeCancelRequest(cancelUrl, requestEntity);
                                latch.countDown();
                            } catch (Exception e) {
                                LOGGER.error("[burstRequest][Burst request {} failed]", burstId, e);
                                latch.countDown();
                            }
                        });
                    }
                    
                    latch.await(1, TimeUnit.SECONDS);
                }
            }

            return response;

        } catch (Exception e) {
            LOGGER.error("[cancelOrder][Cancel order failed][Error: {}]", e.getMessage());
            return new Response<>(0, "Cancel order failed: " + e.getMessage(), null);
        }
    }

    private Response handleOrderCancellation(Order order, String loginId, HttpHeaders headers) {
        if (order.getStatus() == OrderStatus.NOTPAID.getCode()
                || order.getStatus() == OrderStatus.PAID.getCode() 
                || order.getStatus() == OrderStatus.CHANGE.getCode()) {

            Response changeOrderResult;
            if (order.getStatus() == OrderStatus.NOTPAID.getCode()) {
                changeOrderResult = cancelFromOrder(order, headers);
            } else {
                changeOrderResult = cancelFromOtherOrder(order, headers);
            }

            if (changeOrderResult.getStatus() == 1) {
                String money = calculateRefund(order);
                if (drawbackMoney(money, loginId, headers)) {
                    sendNotification(order, headers);
                    return new Response<>(1, "Success.", null);
                }
            }
            return new Response<>(0, changeOrderResult.getMsg(), null);
        }
        return new Response<>(0, orderStatusCancelNotPermitted, null);
    }

    private void sendNotification(Order order, HttpHeaders headers) {
        Response<User> userResult = getAccount(order.getAccountId().toString(), headers);
        if (userResult.getStatus() == 1) {
            NotifyInfo notifyInfo = createNotificationInfo(order, userResult.getData());
            sendEmail(notifyInfo, headers);
        }
    }

    private NotifyInfo createNotificationInfo(Order order, User user) {
        NotifyInfo notifyInfo = new NotifyInfo();
        notifyInfo.setDate(new Date().toString());
        notifyInfo.setEmail(user.getEmail());
        notifyInfo.setStartPlace(order.getFrom());
        notifyInfo.setEndPlace(order.getTo());
        notifyInfo.setUsername(user.getUserName());
        notifyInfo.setSeatNumber(order.getSeatNumber());
        notifyInfo.setOrderNumber(order.getId().toString());
        notifyInfo.setPrice(order.getPrice());
        notifyInfo.setSeatClass(SeatClass.getNameByCode(order.getSeatClass()));
        notifyInfo.setStartTime(order.getTravelTime().toString());
        return notifyInfo;
    }


    public boolean sendEmail(NotifyInfo notifyInfo, HttpHeaders headers) {
        CancelServiceImpl.LOGGER.info("[sendEmail][Send Email]");
        HttpHeaders newHeaders = getAuthorizationHeadersFrom(headers);
        HttpEntity requestEntity = new HttpEntity(notifyInfo, newHeaders);
        String notification_service_url = getServiceUrl("ts-notification-service");
        ResponseEntity<Boolean> re = restTemplate.exchange(
                notification_service_url + "/api/v1/notifyservice/notification/order_cancel_success",
                HttpMethod.POST,
                requestEntity,
                Boolean.class);
        return re.getBody();
    }

    @Override
    public Response calculateRefund(String orderId, HttpHeaders headers) {

        Response<Order> orderResult = getOrderByIdFromOrder(orderId, headers);
        if (orderResult.getStatus() == 1) {
            Order order =   orderResult.getData();
            if (order.getStatus() == OrderStatus.NOTPAID.getCode()
                    || order.getStatus() == OrderStatus.PAID.getCode()) {
                if (order.getStatus() == OrderStatus.NOTPAID.getCode()) {
                    CancelServiceImpl.LOGGER.info("[calculateRefund][Cancel Order, Refund Price From Order Service.Not Paid][orderId: {}]", orderId);
                    return new Response<>(1, "Success. Refoud 0", "0");
                } else {
                    CancelServiceImpl.LOGGER.info("[calculateRefund][Cancel Order, Refund Price From Order Service.Paid][orderId: {}]", orderId);
                    return new Response<>(1, "Success. ", calculateRefund(order));
                }
            } else {
                CancelServiceImpl.LOGGER.info("[calculateRefund][Cancel Order Refund Price Order.Cancel Not Permitted][orderId: {}]", orderId);
                return new Response<>(0, "Order Status Cancel Not Permitted, Refound error", null);
            }
        } else {

            Response<Order> orderOtherResult = getOrderByIdFromOrderOther(orderId, headers);
            if (orderOtherResult.getStatus() == 1) {
                Order order =   orderOtherResult.getData();
                if (order.getStatus() == OrderStatus.NOTPAID.getCode()
                        || order.getStatus() == OrderStatus.PAID.getCode()) {
                    if (order.getStatus() == OrderStatus.NOTPAID.getCode()) {
                        CancelServiceImpl.LOGGER.info("[calculateRefund][Cancel Order, Refund Price From Order Other Service.Not Paid][orderId: {}]", orderId);
                        return new Response<>(1, "Success, Refound 0", "0");
                    } else {
                        CancelServiceImpl.LOGGER.info("[Cancel Order][Refund Price From Order Other Service.Paid][orderId: {}]", orderId);
                        return new Response<>(1, "Success", calculateRefund(order));
                    }
                } else {
                    CancelServiceImpl.LOGGER.warn("[Cancel Order][Refund Price, Order Other. Cancel Not Permitted][orderId: {}]", orderId);
                    return new Response<>(0, orderStatusCancelNotPermitted, null);
                }
            } else {
                CancelServiceImpl.LOGGER.error("[Cancel Order][Refund Price][Order not found][orderId: {}]", orderId);
                return new Response<>(0, "Order Not Found", null);
            }
        }
    }

    private String calculateRefund(Order order) {
        if (order.getStatus() == OrderStatus.NOTPAID.getCode()) {
            return "0.00";
        }
        CancelServiceImpl.LOGGER.info("[calculateRefund][Cancel Order][Order Travel Date: {}]", order.getTravelDate().toString());
        Date nowDate = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(StringUtils.String2Date(order.getTravelDate()));
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(StringUtils.String2Date(order.getTravelTime()));
        int hour = cal2.get(Calendar.HOUR);
        int minute = cal2.get(Calendar.MINUTE);
        int second = cal2.get(Calendar.SECOND);
        Date startTime = new Date(year,  //NOSONAR
                month,
                day,
                hour,
                minute,
                second);
        CancelServiceImpl.LOGGER.info("[calculateRefund][Cancel Order][nowDate  : {}]", nowDate);
        CancelServiceImpl.LOGGER.info("[calculateRefund][Cancel Order][startTime: {}]", startTime);
        if (nowDate.after(startTime)) {
            CancelServiceImpl.LOGGER.warn("[calculateRefund][Cancel Order, Ticket expire refund 0]");
            return "0";
        } else {
            double totalPrice = Double.parseDouble(order.getPrice());
            double price = totalPrice * 0.8;
            DecimalFormat priceFormat = new java.text.DecimalFormat("0.00");
            String str = priceFormat.format(price);
            CancelServiceImpl.LOGGER.info("[calculateRefund][calculate refund][refund: {}]", str);
            return str;
        }
    }


    private Response cancelFromOrder(Order order, HttpHeaders headers) {
        CancelServiceImpl.LOGGER.info("[cancelFromOrder][Change Order Status]");
        order.setStatus(OrderStatus.CANCEL.getCode());
        // add authorization header
        HttpHeaders newHeaders = getAuthorizationHeadersFrom(headers);
        HttpEntity requestEntity = new HttpEntity(order, newHeaders);
        String order_service_url = getServiceUrl("ts-order-service");
        ResponseEntity<Response> re = restTemplate.exchange(
                order_service_url + "/api/v1/orderservice/order",
                HttpMethod.PUT,
                requestEntity,
                Response.class);

        return re.getBody();
    }

    public static HttpHeaders getAuthorizationHeadersFrom(HttpHeaders oldHeaders) {
        HttpHeaders newHeaders = new HttpHeaders();
        if (oldHeaders.containsKey(HttpHeaders.AUTHORIZATION)) {
            newHeaders.add(HttpHeaders.AUTHORIZATION, oldHeaders.getFirst(HttpHeaders.AUTHORIZATION));
        }
        return newHeaders;
    }


    private Response cancelFromOtherOrder(Order order, HttpHeaders headers) {
        CancelServiceImpl.LOGGER.info("[cancelFromOtherOrder][Change Order Status]");
        order.setStatus(OrderStatus.CANCEL.getCode());
        HttpHeaders newHeaders = getAuthorizationHeadersFrom(headers);
        HttpEntity requestEntity = new HttpEntity(order, newHeaders);
        String order_other_service_url = getServiceUrl("ts-order-other-service");
        ResponseEntity<Response> re = restTemplate.exchange(
                order_other_service_url + "/api/v1/orderOtherService/orderOther",
                HttpMethod.PUT,
                requestEntity,
                Response.class);

        return re.getBody();
    }

    public boolean drawbackMoney(String money, String userId, HttpHeaders headers) {
        CancelServiceImpl.LOGGER.info("[drawbackMoney][Draw Back Money]");

        HttpHeaders newHeaders = getAuthorizationHeadersFrom(headers);
        HttpEntity requestEntity = new HttpEntity(newHeaders);
        String inside_payment_service_url = getServiceUrl("ts-inside-payment-service");
        ResponseEntity<Response> re = restTemplate.exchange(
                inside_payment_service_url + "/api/v1/inside_pay_service/inside_payment/drawback/" + userId + "/" + money,
                HttpMethod.GET,
                requestEntity,
                Response.class);
        Response result = re.getBody();

        return result.getStatus() == 1;
    }

    public Response<User> getAccount(String orderId, HttpHeaders headers) {
        CancelServiceImpl.LOGGER.info("[getAccount][Get By Id][orderId: {}]", orderId);
        HttpHeaders newHeaders = getAuthorizationHeadersFrom(headers);
        HttpEntity requestEntity = new HttpEntity(newHeaders);
        String user_service_url = getServiceUrl("ts-user-service");
        ResponseEntity<Response<User>> re = restTemplate.exchange(
                user_service_url + "/api/v1/userservice/users/id/" + orderId,
                HttpMethod.GET,
                requestEntity,
                new ParameterizedTypeReference<Response<User>>() {
                });
        return re.getBody();
    }

    private Response<Order> getOrderByIdFromOrder(String orderId, HttpHeaders headers) {
        CancelServiceImpl.LOGGER.info("[getOrderByIdFromOrder][Get Order][orderId: {}]", orderId);
        HttpHeaders newHeaders = getAuthorizationHeadersFrom(headers);
        HttpEntity requestEntity = new HttpEntity(newHeaders);
        String order_service_url = getServiceUrl("ts-order-service");
        ResponseEntity<Response<Order>> re = restTemplate.exchange(
                order_service_url + "/api/v1/orderservice/order/" + orderId,
                HttpMethod.GET,
                requestEntity,
                new ParameterizedTypeReference<Response<Order>>() {
                });
        return re.getBody();
    }

    private Response<Order> getOrderByIdFromOrderOther(String orderId, HttpHeaders headers) {
        CancelServiceImpl.LOGGER.info("[getOrderByIdFromOrderOther][Get Order][orderId: {}]", orderId);
        HttpHeaders newHeaders = getAuthorizationHeadersFrom(headers);
        HttpEntity requestEntity = new HttpEntity(newHeaders);
        String order_other_service_url = getServiceUrl("ts-order-other-service");
        ResponseEntity<Response<Order>> re = restTemplate.exchange(
                order_other_service_url + "/api/v1/orderOtherService/orderOther/" + orderId,
                HttpMethod.GET,
                requestEntity,
                new ParameterizedTypeReference<Response<Order>>() {
                });
        return re.getBody();
    }

}
