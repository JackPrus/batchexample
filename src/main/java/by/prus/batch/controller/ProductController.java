package by.prus.batch.controller;

import by.prus.batch.model.Product;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@RestController
public class ProductController {

    /**
     * эти мтеоды нужно создать в отдельном проекте и обращаться к нему через
     * @see by.prus.batch.service.ProductService т.к. если через рест вызывать сам себя
     * то будет ошибка.
     * @return
     */

    @GetMapping("/gettestproducts")
    public List<Product> getProducts(){
        List<Product> products = new ArrayList<>();
        Product product1 = new Product(17, "Planeta", "radio system froom web", new BigDecimal("15.50"), 8);
        Product product2 = new Product(17, "Dynamo", "refrigirator from web", new BigDecimal("19.00"), 2);
        products.add(product1);
        products.add(product2);
        return products;
    }

    //http://localhost:8090/batch/product
    @GetMapping("/product")
    public Product getProduct(){
        return new Product(1,"Mak book pro 14.0", "makentosh book", BigDecimal.valueOf(300.00), 10);
    }
}
