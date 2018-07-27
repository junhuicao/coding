import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataEtl {

  private static final Logger LOG = LoggerFactory.getLogger(DataEtl.class);

  public static Properties prop = Util.loadPropertiesFile(System.getProperty("user.dir")+File.separator+"server.prop");
  public static ThreadPoolExecutor executor;
  public static LinkedBlockingQueue<RowBatchResult> rsBatchQueue = new LinkedBlockingQueue<RowBatchResult>(
      Integer.parseInt(prop.getProperty("queueSize")));
  public static ConcurrentHashMap<String, HashMap<String, String>> etlTaskInfoMap = new ConcurrentHashMap<String, HashMap<String, String>>();

  public static void main(String[] args)
  {
    String dbconfPath = System.getProperty("user.dir")+File.separator+"dbconf.prop";
    //Properties prop = loadPropertiesFile(path);
    //System.out.println(path);
    LOG.info("user.dir: "+System.getProperty("user.dir"));
    Util util = new Util();
    Util.GetDbConnect(util.readDbConfFile(dbconfPath));


    executor = new ThreadPoolExecutor(Integer.parseInt(prop.getProperty("ThreadCorePoolSize"))
        ,Integer.parseInt(prop.getProperty("ThreadMaxPoolSize"))
        ,Integer.parseInt(prop.getProperty("ThreadKeepAliveTime"))
        , TimeUnit.MILLISECONDS
        ,new ArrayBlockingQueue<Runnable>(Integer.parseInt(prop.getProperty("ThreadQueueSize"))));

    /*new Thread(new InfoPrint(executor,rsBatchQueue,etlTaskInfoMap)).start();*/

    int consumerThreadNum = Integer.parseInt(prop.getProperty("consumerThreadNum"));
    for(int i=0;i<consumerThreadNum;i++)
    {
      new Thread(new ToFtpConsumer(rsBatchQueue,etlTaskInfoMap,prop)).start();
    }

    Timer timer = new Timer();
    MyTimerTask timerTask = new MyTimerTask();
    timer.scheduleAtFixedRate(timerTask,new Date(),Integer.parseInt(prop.getProperty("interval"))*1000);

/*    try {
      Util.writeOrc();
    } catch (IOException e) {
      e.printStackTrace();
    }*/
  }

  public static void threadPoolStatus(){

    LOG.info("线程池中线程数目：" + executor.getPoolSize() + "，队列中等待执行的任务数目：" +
        executor.getQueue().size() + "，已执行完成任务数目：" + executor.getCompletedTaskCount()
        +", 线程队列："+executor.getQueue());
  }



  public static void scheduler() {

    Connection connection = null;
    PreparedStatement ps = null;

    try {

      connection = Util.getConnect(prop.getProperty("etlConn"));
      ps = connection.prepareStatement(prop.getProperty("scheduleSql"));
      ResultSet resultSet = ps.executeQuery();
      ResultSetMetaData rsMeta = resultSet.getMetaData();
      int columnCnt = rsMeta.getColumnCount();

      while (resultSet.next()) {

        Map<String, String> rsMap = new HashMap<String, String>();

        for (int i = 1; i <= columnCnt; i++) {
          rsMap.put(rsMeta.getColumnName(i), resultSet.getString(i));
        }

        int scheduleCnt;
        if (!etlTaskInfoMap.containsKey(resultSet.getString("id"))) {
          scheduleCnt = 1;
          HashMap<String, String> globalTaskInfo = new HashMap<String, String>();
          globalTaskInfo.put("schedulerCnt", String.valueOf(scheduleCnt));
          etlTaskInfoMap.put(resultSet.getString("id")
              , globalTaskInfo);
        } else {

          HashMap<String, String> globalTaskInfo = etlTaskInfoMap.get(resultSet.getString("id"));
          scheduleCnt = Integer.parseInt(globalTaskInfo.get("schedulerCnt"));
          if(etlTaskInfoMap.containsKey(resultSet.getString("id")+"_"+scheduleCnt))
          {
            LOG.warn("抽取任务："+resultSet.getString("id")+"的第"+scheduleCnt+"调度还未执行完成。");
            continue;
          }
          scheduleCnt = scheduleCnt + 1;
          globalTaskInfo.put("schedulerCnt"
              , String.valueOf(scheduleCnt));
          etlTaskInfoMap.put(resultSet.getString("id")
              , globalTaskInfo);
        }

        HashMap<String, String> taskInfoMap = new HashMap<String, String>();
        taskInfoMap.put("id", resultSet.getString("id"));
        taskInfoMap.put("taskId",resultSet.getString("id")+"_"+scheduleCnt);
        taskInfoMap.put("taskName",
            resultSet.getString("dbname")
                + "_" + resultSet.getString("tablespace")
                + "_" + resultSet.getString("table")
        );
        taskInfoMap.put("beginTime", Util.getDateStr());
        taskInfoMap.put("endTime", Util.getDateStr());
        taskInfoMap.put("beginValue",resultSet.getString("fieldBeginValue"));
        taskInfoMap.put("endValue",resultSet.getString("fieldEndValue"));
        taskInfoMap.put("status", "success");
        taskInfoMap.put("remark", "");
        etlTaskInfoMap.put(
            resultSet.getString("id") + "_" + scheduleCnt
            , taskInfoMap
        );
        executor.execute(new MyTask(rsMap, rsBatchQueue, etlTaskInfoMap
            , resultSet.getString("id") + "_" + scheduleCnt));
      }

      resultSet.close();
      ps.close();
      connection.close();
      //executor.shutdown();

    } catch (Exception e) {
      e.printStackTrace();
    }finally {
      if(ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if(connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }

  }
}


