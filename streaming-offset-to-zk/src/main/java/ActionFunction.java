import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;

public class ActionFunction implements VoidFunction<Iterator<String>> {

    @Override
    public void call(Iterator<String> iter) throws Exception {

    }
}
