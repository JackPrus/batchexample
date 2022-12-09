package by.prus.batch.processor;

import by.prus.batch.model.Product;
import org.springframework.batch.item.ItemProcessor;

public class ProductProcessor implements ItemProcessor<Product, Product> {

    /**
     * Метод удаляет объект если у него ID=2. И переводит описание в upercase
     * у всех остальных.
     * @param product
     * @return
     * @throws Exception
     */
    @Override
    public Product process(Product product) throws Exception {
        if (product.getProductId()==2){
            throw new RuntimeException("the processor ID is 2");
        }
       product.setProductDesc(product.getProductDesc().toUpperCase());
       return product;
    }
}
