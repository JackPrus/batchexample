package by.prus.batch.reader;

import by.prus.batch.model.Product;
import by.prus.batch.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ProductExceptionServiceAdapter {

    Logger logger = LoggerFactory.getLogger(ProductServiceAdapter.class);

    @Autowired
    ProductService service;

    public Product nextProduct() throws InterruptedException {

        Product p = null;
        Thread.sleep(3000);
        try {
            p = service.getProduct();
            logger.info("connected web service .... ok");
        }catch(Exception e){
            logger.info("exception ..." + e.getMessage());
            throw e;
        }
        return p;
    }
}
