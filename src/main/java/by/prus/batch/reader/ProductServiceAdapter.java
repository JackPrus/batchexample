package by.prus.batch.reader;

import by.prus.batch.model.Product;
import by.prus.batch.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ProductServiceAdapter implements InitializingBean {

    @Autowired
    private ProductService productService;
    private List<Product> products;

    Logger logger = LoggerFactory.getLogger(ProductServiceAdapter.class);

    @Override
    public void afterPropertiesSet() throws Exception {
        this.products = productService.getProducts();
    }

    public Product nextProduct() {
        return products.isEmpty() ? null : products.remove(0);
    }

    public ProductService getProductService() {
        return productService;
    }
    public void setProductService(ProductService productService) {
        this.productService = productService;
    }
    public List<Product> getProducts() {
        return products;
    }
    public void setProducts(List<Product> products) {
        this.products = products;
    }
}
