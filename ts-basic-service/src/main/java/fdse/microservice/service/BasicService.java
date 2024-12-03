package fdse.microservice.service;

import edu.fudan.common.entity.Travel;
import edu.fudan.common.util.Response;
import edu.fudan.common.entity.*;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;

import java.util.List;

/**
 * @author Chenjie
 * @date 2017/6/6.
 */
public interface BasicService {

    String getBurstParams(@RequestHeader HttpHeaders headers);

    HttpEntity setBurstParams(@RequestBody List<Integer> params, @RequestHeader HttpHeaders headers);

    /**
     * query for travel with travel information
     *
     * @param info information
     * @param  headers headers
     * @return Response
     */
    Response queryForTravel(Travel info, HttpHeaders headers);

    Response queryForTravels(List<Travel> infos, HttpHeaders headers);

    /**
     * query for station id with station name
     *
     * @param stationName station name
     * @param  headers headers
     * @return Response
     */
    Response queryForStationId(String stationName, HttpHeaders headers);
}
