package by.prus.batch.tasklet;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.util.Random;

public class BizTasklet4 implements Tasklet, StepExecutionListener {
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        System.out.println("Business task 4 started ...");
        Thread.sleep(1000);
        System.out.println("Business task 4 Completed");
        return RepeatStatus.FINISHED;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {

    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        /*
         *когда FAILED - запустится PageDutyTasklet() , т.к.
         * его запускает этот блок кода в bizStep4()
         *
         * .from(bizStep4()).on("*").end()
                .on("FAILED")
                .to(stepPagerDutyStep())
         */

        ExitStatus [] statuses = new ExitStatus[]{ExitStatus.FAILED, ExitStatus.COMPLETED};
        Random random = new Random(2);
        ExitStatus status = statuses[random.nextInt()];
        if (status==ExitStatus.FAILED){
            System.out.println("Task Failed");
        }
        return status;
    }
}
