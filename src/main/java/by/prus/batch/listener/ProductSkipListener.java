package by.prus.batch.listener;

import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.core.annotation.OnSkipInWrite;
import org.springframework.batch.item.file.FlatFileParseException;

import java.io.FileOutputStream;
import java.io.IOException;

public class ProductSkipListener {

    private String readerrFileName ="errors/read_skipped";
    private String proceserrFileName ="errors/process_skipped";

    @OnSkipInRead
    public void onSkipRead(Throwable t){
        if ( t instanceof FlatFileParseException){
            FlatFileParseException ffep = (FlatFileParseException) t;
            onSkip(ffep.getInput(),readerrFileName );
        }
    }

    //когда ошибка в Processor тогда сробатывает
    @OnSkipInProcess
    public void onSkipinProcess(Object item, Throwable t){
        if ( t instanceof RuntimeException){
            onSkip(item,proceserrFileName );
        }
    }

    //когда ошибка в Writer - тогда срабатывает этот метод
    @OnSkipInWrite
    public void onSkipinWrite(Object item, Throwable t){
        if ( t instanceof RuntimeException){
            onSkip(item,proceserrFileName );
        }
    }

    public void onSkip(Object o, String fname){
        FileOutputStream fos = null;

        try {
            fos = new FileOutputStream(fname, true);
            fos.write(o.toString().getBytes());
            fos.write("\r\n".getBytes());
            fos.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
