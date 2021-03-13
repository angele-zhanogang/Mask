import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * 实现具体的业务操作
 */
public class ActionFunction implements VoidFunction<Iterator<String>> ,Serializable{
    @Override
    public void call(Iterator<String> datas) throws Exception {
        //如果没有使用广播变量，即在此处初始化各种连接资源

        while(datas.hasNext()){
            String data = datas.next();
            System.out.println("data:============================"+data);
        }

    }
}
