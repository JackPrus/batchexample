package by.prus.batch.config;

import by.prus.batch.listener.HwJobExecutionListener;
import by.prus.batch.listener.HwStepExecutionListener;
import by.prus.batch.listener.ProductSkipListener;
import by.prus.batch.model.Product;
import by.prus.batch.processor.InMemeItemProcessor;
import by.prus.batch.processor.ProductProcessor;
import by.prus.batch.reader.*;
import by.prus.batch.tasklet.*;
import by.prus.batch.writer.ConsoleItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.batch.item.file.*;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.*;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.ResourceAccessException;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@EnableBatchProcessing
@Configuration
public class BatchCondifguration {

    /**
     * Над методами стоит аннотация @StepScope. Когда мы хотим использовать метод - эту аннотацию
     * нужно разкомментировать.
     * Одна и таже работа не может использваовь несколько разных read методов и участвует в процессе
     * лишь последний read() метод. А аннотация stepScope используется для пошагового использования.
     * То есть если бы в нас в аргументах был параметр а метод read() в своих аргументах получал следующее:
     *
     * @Value( "#{jobParameters['fileInput']}"  ) FileSystemResource inputFile
     * то тогда аннотация @StepScope была бы необходима. А так по сути она мешает, т.к. значение в
     * метод с адресом считываемых данных мы получаем из полей класса помещеченных @Value
     */

    @Autowired
    private JobBuilderFactory jobs;
    @Autowired
    private StepBuilderFactory steps;
    @Autowired
    private HwJobExecutionListener hwJobExecutionListener;
    @Autowired
    private HwStepExecutionListener hwStepExecutionListener;
    @Autowired
    private InMemeItemProcessor inMemeItemProcessor;
    @Autowired
    private DataSource dataSource;
    @Autowired
    private ProductServiceAdapter productServiceAdapter;
    @Autowired
    private ProductExceptionServiceAdapter productExceptionServiceAdapter;

    @Value("input/productwithdel.csv")
    private FileSystemResource inpurCSVFile;
    @Value("input/product.xml")
    private FileSystemResource inputXMLFile;
    @Value("input/productfix.txt")
    private FileSystemResource inputTextFile;
    @Value("input/product.json")
    private FileSystemResource inputJosnFile;
    @Value("output/product_out.csv")
    private FileSystemResource outputCSVFile;
    @Value("output/product_out.xml")
    private FileSystemResource outputXMLFile;

    // Такие же такслеты можно создавать отдельным классом в пакете тасклет и потом из степа идти в этот класс.
    public Tasklet helloWorldTasklet() {
        return (new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Hello world  ");
                return RepeatStatus.FINISHED;
            }
        });
    }

    @Bean
    public Step step1() {
        return steps.get("step1")
                .listener(hwStepExecutionListener)
                .tasklet(helloWorldTasklet())
                .build();
    }

    @Bean
    public Step step2() {
        return steps.get("step2")
                .<Integer, Integer>chunk(3) //означает, что 3 строки read, 3 раза process 3 раза write сделает и перейдет обратно в read
                .reader(fromCsvReader(inpurCSVFile))
                //.reader(fromXmlReader(inputXMLFile))
                //.reader(fromTxtReader(inputTextFile))
                //.reader(fromDbReader())
                //.reader(fromJsonReader(inputJosnFile))
                //.reader(fromWebReader())
                //.reader(fromWebExceptionReader())

                .processor(new ProductProcessor())

                //.writer(writer())
                .writer(writeToCSV(outputCSVFile))
                //.writer(writeToXML(outputXMLFile))
                //.writer(writeToDB())
                //.writer(writeToDB2())

                .faultTolerant()
                .retry(ResourceAccessException.class) //когда качаем с ресурса ресурс может отключиться.
                .retryLimit(3) // 3 раза по отключенному ресурсу еще постучимся, заетм остановим запрос.
                .skip(ResourceAccessException.class)

                .skip(FlatFileParseException.class)
                .skipLimit(10) // количество строк не соответствующих шаблону, которое разрешено пропустить без ошибки
                //.skipPolicy(new AlwaysSkipItemSkipPolicy())
                //.listener(new ProductSkipListener())
                .build();
    }

    /**
     * При работе в многопоточной среде лучше создавать таск экзекьютор, дабы не перегрузить систему.
     * В данном случае нет паралельного выполнения. И экзекьютор отрабатывает последовательно
     * в отличие от следующего, использующего AsyncExecytor в процессоре. Который сам работает
     * как многопоточный выполняя step-ы паралельно.
     * @return
     */
    @Bean
    public Step multiThreadStep(){

        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(2);
        taskExecutor.setMaxPoolSize(2);
        taskExecutor.afterPropertiesSet();

        return steps.get("multiThreadStep")
                .<Product,Product>chunk(3)
                .reader(fromCsvReader(inpurCSVFile))
                .processor(new ProductProcessor())
                .writer(writeToCSV(outputCSVFile))

                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(10)
                .taskExecutor(taskExecutor)
                .build();
    }

    /**
     * Асинхронный экзекьютор, работает быстрее чем последовательный.
     * В этом мы можем убедиться посмотрев время выполнения.
     * @return
     */
    @Bean
    public Step asyncStep(){
        return steps.get("asyncStep")
                .<Product,Product>chunk(5)
                .reader(fromCsvReader(inpurCSVFile))
                .processor( asyncItemProcessor())
                .writer(asyncItemWriter())

                .faultTolerant()
                .skipPolicy(new AlwaysSkipItemSkipPolicy())
                .build();
    }

    @Bean
    public AsyncItemProcessor asyncItemProcessor(){
        AsyncItemProcessor processor = new AsyncItemProcessor();
        processor.setDelegate(new ProductProcessor());
        processor.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return processor;
    }

    @Bean
    public AsyncItemWriter asyncItemWriter(){
        AsyncItemWriter writer = new AsyncItemWriter();
        writer.setDelegate(writeToCSV(outputCSVFile));
        return writer;
    }

    //@StepScope
    @Bean
    public FlatFileItemReader fromCsvReader(FileSystemResource inpurCSVFile) {
        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(inpurCSVFile);

        DefaultLineMapper<Product> defaultLineMapper = new DefaultLineMapper<>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames("productID", "productName", "prodcuctDesc", "price", "unit");
                        setDelimiter("|");
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Product>() {
                    {
                        setTargetType(Product.class);
                    }
                });
            }
        };
        reader.setLineMapper(defaultLineMapper);
        reader.setLinesToSkip(1);
        return reader;
    }

    @StepScope //рскомментировать StepScope если мы не будем использовать этот метод
    @Bean
    public StaxEventItemReader fromXmlReader(FileSystemResource inputXMLFile) {
        StaxEventItemReader reader = new StaxEventItemReader();
        Jaxb2Marshaller jaxb2Marshaller = new Jaxb2Marshaller();
        jaxb2Marshaller.setClassesToBeBound(Product.class);
        reader.setUnmarshaller(jaxb2Marshaller);
        reader.setResource(inputXMLFile);
        reader.setFragmentRootElementName("product");
        return reader;
    }

    @StepScope
    @Bean
    public FlatFileItemReader fromTxtReader(FileSystemResource inputTextFile) {
        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
        tokenizer.setNames("prodId", "productName", "productDesc", "price", "unit");
        tokenizer.setColumns(
                new Range(1, 16),
                new Range(17, 41),
                new Range(42, 65),
                new Range(66, 73),
                new Range(74, 80)
        );

        BeanWrapperFieldSetMapper fieldWrapper = new BeanWrapperFieldSetMapper();
        fieldWrapper.setTargetType(Product.class);

        DefaultLineMapper<Product> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldWrapper);

        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(inputTextFile);
        reader.setLineMapper(lineMapper);
        reader.setLinesToSkip(1);
        return reader;
    }

    @StepScope
    @Bean
    public JdbcCursorItemReader fromDbReader() {
        JdbcCursorItemReader reader = new JdbcCursorItemReader();
        reader.setDataSource(this.dataSource);
        reader.setSql("select product_id, product_name, product_desc as productDesc, unit, price from products");
        reader.setRowMapper(new BeanPropertyRowMapper() {
            {
                setMappedClass(Product.class);
            }
        });
        return reader;
    }

    @StepScope
    @Bean
    public JsonItemReader fromJsonReader(FileSystemResource inputFile) {
        JsonItemReader reader = new JsonItemReader(inputFile, new JacksonJsonObjectReader(Product.class));
        return reader;
    }

    @StepScope
    @Bean
    public ItemReaderAdapter fromWebReader() {
        ItemReaderAdapter reader = new ItemReaderAdapter();
        reader.setTargetObject(productServiceAdapter);
        reader.setTargetMethod("nextProduct");
        return reader;
    }

    @StepScope
    @Bean
    public ItemReaderAdapter fromWebExceptionReader() {
        ItemReaderAdapter reader = new ItemReaderAdapter();
        reader.setTargetObject(productExceptionServiceAdapter);
        reader.setTargetMethod("nextProduct");
        return reader;
    }

    @Bean
    public InMemReader reader() {
        return new InMemReader();
    }

    @Bean
    public ConsoleItemWriter consoleWriter() {
        return new ConsoleItemWriter();
    }

    @Bean
    //@StepScope
    public FlatFileItemWriter writeToCSV(FileSystemResource outputFile) {
        BeanWrapperFieldExtractor fieldExtractor = new BeanWrapperFieldExtractor();
        fieldExtractor.setNames(new String[]{"productId", "productName", "productDesc", "price", "unit"});

        DelimitedLineAggregator aggregator = new DelimitedLineAggregator();
        aggregator.setDelimiter("|");
        aggregator.setFieldExtractor(fieldExtractor);

        FlatFileItemWriter writer = new FlatFileItemWriter();
        writer.setResource(outputFile);
        writer.setLineAggregator(aggregator);

        // wtitting the header
        writer.setHeaderCallback(writer1 -> writer1.write("productID,productName,ProductDesc,price,unit"));
        writer.setAppendAllowed(false); //переписать или поверх написанного дописать.
        writer.setFooterCallback(writer2 -> writer2.write(" The file is created at " + new SimpleDateFormat().format(new Date())));
        return writer;
    }

    @Bean
    @StepScope
    public StaxEventItemWriter writeToXML(FileSystemResource outputFile) {
        XStreamMarshaller marshaller = new XStreamMarshaller();
        HashMap<String, Class> aliases = new HashMap<>();
        aliases.put("product", Product.class);
        marshaller.setAliases(aliases);
        marshaller.setAutodetectAnnotations(true);

        StaxEventItemWriter staxEventItemWriter = new StaxEventItemWriter();

        staxEventItemWriter.setResource(outputFile);
        staxEventItemWriter.setMarshaller(marshaller);
        staxEventItemWriter.setRootTagName("products");
        return staxEventItemWriter;
    }

    //используюя prepared statement
    @Bean
    @StepScope
    public JdbcBatchItemWriter writeToDB() {
        JdbcBatchItemWriter writer = new JdbcBatchItemWriter();
        writer.setDataSource(this.dataSource);
        writer.setSql("insert into products (product_id, product_name, product_desc, price, unit)" +
                " values (?, ?, ?, ? , ? ) ");
        writer.setItemPreparedStatementSetter((ItemPreparedStatementSetter<Product>) (item, ps) -> {
            ps.setInt(1, item.getProductId());
            ps.setString(2, item.getProductName());
            ps.setString(3, item.getProductDesc());
            ps.setBigDecimal(4, item.getPrice());
            ps.setInt(5, item.getUnit());
        });
        return writer;
    }

    //Запись в БД используя hql
    @Bean
    //@StepScope
    public JdbcBatchItemWriter writeToDB2() {
        return new JdbcBatchItemWriterBuilder<Product>()
                .dataSource(this.dataSource)
                .sql("insert into products (product_id, product_name, product_desc, price, unit )" +
                        " values ( :productId, :productName, :productDesc, :price, :unit ) ")
                .beanMapped()
                .build();
    }

    @Bean
    public Job helloWorldJob() {
        return jobs.get("helloWorldJob")
                // в program arguments добавить start_dt=2022-26, чтобы была возможность каждый раз делать одну и ту же job
                .incrementer(new RunIdIncrementer())
                .listener(hwJobExecutionListener)
                .start(step1())
                .next(step2())
                .next(multiThreadStep())
                .next(asyncStep())
                .build();
    }



    /**
     * Нжие - конфигурационный блок для паралельной работы. Он не завсит от всего что выше.
     * Тут мы создаем пошаговую работу. Скачка, обработка, бизнес процессы , очистка.
     *     download - downloadstep
     *     process file  - process filestep
     *     process another business items - businessTask3
     *     businesstask3  - businesstask4
     *     clean up step  -cleanuptask
     */


    /**
     * Ниже созданы шаги. Этот метод выполняет последовательную работу по шагам.
     * И весь job выполняется за 3,5 секунд. Ниже предоставлен улучшеный job
     */
    @Bean
    public Job paraleleTaskLetJob(){
        return jobs.get("paraleleTaskLetJob")
                .incrementer(new RunIdIncrementer())
                .start(downloadStep())
                .next(fileProcessStep())
                .next(bizStep3())
                .next(bizStep4())
                .next(cleanUpStep())
                .build();
    }

    /**
     * Этот же Job, как и Job выше выполняет одну и ту же работу. Но здесь мы
     * создаем Flows и каждый или несколько steps закидываем во Flow.
     * Затем главный Flow (splitFlow) объединяет эти step-ы и при помощи
     * new SimpleAsyncTaskExecutor() запускает их паралельно. Что сказывается в
     * конечном итоге на скорость работы всего batch-процесса. То есть эта Job
     * выполняется гораздо быстрее , чем последовательная job выше (paraleleTaskLetJob)
     */
    @Bean
    public Job paraleleFlowJob(){
        return jobs.get("paraleleFlowJob")
                .incrementer(new RunIdIncrementer())
                .start(splitFlow())
                .next(cleanUpStep())
                .end()
                .build();
    }

    public Step downloadStep(){
        return steps.get("downloadstep")
                .tasklet(new DownloadTasklet())
                .build();
    }

    public Step fileProcessStep(){
        return steps.get("fileProcessStep")
                .tasklet(new FileProcessTasklet())
                .build();
    }


    public Step bizStep3(){
        return steps.get("bizStep3")
                .tasklet(new BizTasklet3())
                .build();
    }

    public Step bizStep4(){
        return steps.get("bizStep4")
                .tasklet(new BizTasklet4())
                .build();
    }

    public Step cleanUpStep(){
        return steps.get("clenaUpStep")
                .tasklet(new CleanupTasklet())
                .build();
    }

    public Step stepPagerDutyStep(){
        return steps.get("stepPagerDutyStep")
                .tasklet(new PageDutyTasklet())
                .build();
    }

    /**
     * Этот флов, объединяет все другие flows и с помощью SimpleAsyncTaskExecutor
     * Запускает все процессы параелельно. Что увеличивает скорость выполнения работы
     * в отличие от последовательного способоа по шагам выполненного в paraleleTaskLetJob
     * @return
     */
    public Flow splitFlow(){
        return new FlowBuilder<SimpleFlow>("splitFlow")
                .split(new SimpleAsyncTaskExecutor())
                .add(fileFlow(),bizFlow1(),bizFlow2())
                .build();
    }

    public Flow fileFlow(){
        return new FlowBuilder< SimpleFlow > ("fileFlow")
                .start(downloadStep())
                .next(fileProcessStep())
                .build();
    }

    public Flow bizFlow1(){
        return new FlowBuilder< SimpleFlow > ("bizFlow1")
                .start(bizStep3())
                .build();
    }

    public Flow bizFlow2(){
        return new FlowBuilder< SimpleFlow > ("bizFlow2")
                .start(bizStep4())//начинаем выполнять степ
                .from(bizStep4()).on("*").end() // если все ок, заканчиваем
                .on("FAILED") // если ExitStatus.FAILED метода afterStep() класса BizTasklet4()
                .to(stepPagerDutyStep()) // то мы выполняем этот степ со своим тасклетом
                .build();
    }

    /**
     * Дельше следует независимый блок блок с партициями.
     * @return
     */

    @Bean
    public Job partitionJob(){
        return jobs.get("partitionJob")
                .incrementer(new RunIdIncrementer())
                .start(partitionStep())
                .build();
    }

    @Bean
    @StepScope  //for good job we need @stepscope annatation is active here
    public JdbcPagingItemReader pagingDatabaseItemReader(){
        Long minValue = 1L; // с 5 по 10 ид нам достанет и запишет в консоль
        Long maxValue = 3L;
        System.out.println("From " + minValue + " to "+ maxValue );
        Map<String, Order> sortKey = new HashMap<>();
        sortKey.put("product_id", Order.ASCENDING);

        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("product_id, product_name, product_desc, unit, price");
        queryProvider.setFromClause("from products");
        queryProvider.setWhereClause("where product_id >=" + minValue + " and product_id <" + maxValue);
        queryProvider.setSortKeys(sortKey);

        JdbcPagingItemReader reader = new JdbcPagingItemReader();
        reader.setDataSource(this.dataSource);
        reader.setQueryProvider(queryProvider);
        reader.setFetchSize(1000);

        reader.setRowMapper(new BeanPropertyRowMapper(){
            {
                setMappedClass(Product.class);
            }
        });
        return reader;
    }

    /**
     * Когда мы устанавливаем гридсайз назначаются потоки.
     * И чтобы было быстрее мы устанавливаем большой гридсайз
     * Смотри partitionStep()
     * В случае же этого партиционера смотри имплементацию
     * вычисляется размер базы данных и каждому потоку назначается
     * определенный размер строк, который он обработает.
     * Это избавляет нас от большого числа потоков в gridSize
     * и можно обойтись 2-3 потоками.
     * @return
     */
    public ColumnRangePartitioner columnRangePartitioner(){
        ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();
        columnRangePartitioner.setColumn("product_id");;
        columnRangePartitioner.setDataSource(dataSource);
        columnRangePartitioner.setTable("products");
        return columnRangePartitioner;
    }

    public Step slaveStep(){
        return steps.get("slaveStep")
                .<Product, Product> chunk(5)//количество строк чтение/процесс/запись в рамкох одной процедуры.
                .reader(pagingDatabaseItemReader())
                .writer(consoleWriter())
                .build();
    }

    public Step partitionStep(){
        return steps.get("partitionStep")
                //.partitioner(slaveStep().getName(), new RangePartitioner()) // можно использовать и этот партитионер
                .partitioner(slaveStep().getName(), columnRangePartitioner())
                .step(slaveStep())
                .gridSize(2) //разбивает на потоки работы считывания/записи. Если 15 записей в базе - то будет работать 3 потока по 5 записей на каждый
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }

}
