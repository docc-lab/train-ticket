package consignprice.init;

import consignprice.entity.ConsignPrice;
import consignprice.service.ConsignPriceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * @author fdse
 */
@Component
public class InitData implements CommandLineRunner {
    @Autowired
    ConsignPriceService service;

    private static final Logger LOGGER = LoggerFactory.getLogger(InitData.class);

    @Override
    public void run(String... strings) throws Exception {
        try {
            InitData.LOGGER.info("[InitData.run][Consign price service][Init data operation]");
            ConsignPrice config = new ConsignPrice();
            config.setId(UUID.randomUUID().toString());
            config.setIndex(0);
            config.setInitialPrice(8);
            config.setInitialWeight(1);
            config.setWithinPrice(2);
            config.setBeyondPrice(4);

            Response response = service.createAndModifyPrice(config, null);
            if (response.getStatus() == 0) {
                LOGGER.warn("[InitData.run] Failed to initialize price config: {}", response.getMessage());
            } else {
                LOGGER.info("[InitData.run] Successfully initialized/updated price config");
            }
        } catch (Exception e) {
            // Log error but don't fail application startup
            LOGGER.error("[InitData.run] Error during initialization", e);
        }
    }
}
