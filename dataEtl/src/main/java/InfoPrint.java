import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfoPrint implements  Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(InfoPrint.class);

  private ThreadPoolExecutor executor;
  private LinkedBlockingQueue<RowBatchResult> rsBatchQueue;
  private ConcurrentHashMap<String, HashMap<String, String>> etlTaskInfoMap;
  InfoPrint(ThreadPoolExecutor executor,LinkedBlockingQueue<RowBatchResult> rsBatchQueue,ConcurrentHashMap<String, HashMap<String, String>> etlTaskInfoMap)
  {
    this.executor = executor;
    this.etlTaskInfoMap = etlTaskInfoMap;
    this.rsBatchQueue = rsBatchQueue;
  }

  @Override
  public void run() {
    while (true)
    {
      try {
        LOG.info("线程池中线程数目：" + executor.getPoolSize() + "，队列中等待执行的任务数目：" +
            executor.getQueue().size() + "，已执行完成任务数目：" + executor.getCompletedTaskCount()
            +", 线程池："+executor
            +", 线程队列："+executor.getQueue());

        LOG.info("XXXXXX queue ---> "+rsBatchQueue.size());
        LOG.info("XXXXXX taskInfo ---> "+etlTaskInfoMap.toString());
        Thread.sleep(30000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
