package seat.service;

import edu.fudan.common.util.Response;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import edu.fudan.common.entity.Seat;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;

import java.util.List;

/**
 * @author fdse
 */
public interface SeatService {

    String getBurstParams(@RequestHeader HttpHeaders headers);

    HttpEntity setBurstParams(@RequestBody List<Integer> params, @RequestHeader HttpHeaders headers);

    Response distributeSeat(Seat seatRequest, HttpHeaders headers);
    Response getLeftTicketOfInterval(Seat seatRequest, HttpHeaders headers);
}
