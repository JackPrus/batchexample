package by.prus.batch.service;

import by.prus.batch.model.Product;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.net.ConnectException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ProductService {

    public List<Product> getProducts(){
        try{
            RestTemplate restTemplate = new RestTemplate();
            //TODO сменить ссылку на другой проект, чтобы доставать от туда лист Product-ов.
            String url = "http://localhost:8080/batch/gettestproducts";
            Product[] products = restTemplate.getForObject(url, Product[].class);
            return products!=null ? Arrays.stream(products).collect(Collectors.toList()) : null;
        }catch (ResourceAccessException | HttpClientErrorException e){
            System.out.println("Недоступно соединение, проверьте работоспособность ссылки.");
            e.printStackTrace();
            return null;
        }
    }

    public Product getProduct(){
        RestTemplate restTemplate = new RestTemplate();
        String url ="http://localhost:8080/product";
        Product p = restTemplate.getForObject(url, Product.class);
        return p;
    }
}
