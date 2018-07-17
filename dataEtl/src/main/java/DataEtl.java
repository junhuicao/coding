import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.SimpleFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataEtl {

  private static final Logger LOG = LoggerFactory.getLogger(DataEtl.class);

  public static Properties prop = loadPropertiesFile(System.getProperty("user.dir")+File.separator+"server.prop");
  public static ThreadPoolExecutor executor;
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

    Timer timer = new Timer();
    MyTimerTask timerTask = new MyTimerTask();
    timer.scheduleAtFixedRate(timerTask,new Date(),Integer.parseInt(prop.getProperty("interval"))*1000);

 /*   Util.upload(Util.Ftpconnect("/test","127.0.0.1",21,"ftp","ftp"),
        new File("D:\\TCLOUD.T_OGG2018-07-14"));*/
    //Util.write2blob();
    //Util.readFile();
    //scheduler("inc");
    //execute("ogg",passCarSql);
    //execute("mysql",mysqlSql);
    //executor();
  }

  public static void threadPoolStatus(){

    LOG.info("线程池中线程数目：" + executor.getPoolSize() + "，队列中等待执行的任务数目：" +
        executor.getQueue().size() + "，已执行完成任务数目：" + executor.getCompletedTaskCount());
  }



  public static void scheduler(String etlType) {

    Connection connection = null;
    PreparedStatement ps = null;
    try {

      connection = Util.getConnect("mysql");
      ps = connection.prepareStatement(prop.getProperty("scheduleSql"));
      ResultSet resultSet = ps.executeQuery();


      while(resultSet.next()){
        String sql = "";

        LOG.info(resultSet.getString("isAll"));

        if((null == resultSet.getString("updateSql")) || (resultSet.getString("updateSql").length() == 0)){
          sql = "select * from "+resultSet.getString("tablespace")
              +"."+resultSet.getString("table")
              +" where 1=1 ";
        }
        else{
          sql = resultSet.getString("updateSql");
        }


        if("fill"==etlType)
        {
          sql = sql + " and fieldBeginValue >= '"+resultSet.getString("fieldBeginValue")
              +"' and fieldBeginValue < '"+resultSet.getString("fieldEndValue") +"'";
          executor.execute(new MyTask(prop,resultSet.getString("dbname")
              ,resultSet.getString("tablespace")+"."+resultSet.getString("table")
              ,sql,resultSet.getString("id"),null));

          threadPoolStatus();
        }
        else if("his"==etlType)
        {
          sql = sql + " and fieldInitValue < '"+resultSet.getString("fieldInitValue")+"'";
          executor.execute(new MyTask(prop,resultSet.getString("dbname")
              ,resultSet.getString("tablespace")+"."+resultSet.getString("table")
              ,sql,resultSet.getString("id"),null));

          threadPoolStatus();
        }
        else
        {
          if("all".equals(resultSet.getString("isAll")))
          {

            LOG.info("sql: "+sql);

            executor.execute(new MyTask(prop,resultSet.getString("dbname")
                ,resultSet.getString("tablespace")+"."+resultSet.getString("table")
                ,sql,resultSet.getString("id"),null));

            threadPoolStatus();
          }
          else{
            if("T+1".equals(resultSet.getString("updateType")) && "time".equals(resultSet.getString("updateFieldType")) )
            {
              SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
              Date toDay = new Date();
              Date yesterDay  = new Date(toDay.getTime() - 86400000L);
              String toDayStr = sdf.format(toDay)+" 00:00:00";
              String yesterDayStr = sdf.format(yesterDay) + " 00:00:00";

              String updateSql = "update "+prop.getProperty("etldb")+"."
                  +prop.getProperty("etltable")
                  +" set fieldEndValue = '"+toDayStr+"',fieldBeginVAlue = '" + yesterDayStr + "'";
              if(null == resultSet.getString("fieldInitValue") || "".equals(resultSet.getString("fieldInitValue")))
              {
                updateSql = updateSql+",fieldInitValue = '"+yesterDayStr+"'";
              }

              updateSql = updateSql + " where id = " + resultSet.getString("id");

              new Util().update("mysql",updateSql);

              sql = sql + " and "+resultSet.getString("updateField")+" >= to_date('"
                  + yesterDayStr + "','YYYY-MM-DD HH24:MI:SS') and "+resultSet.getString("updateField")
                  + " < to_date('" + toDayStr + "','YYYY-MM-DD HH24:MI:SS')";

              LOG.info("sql: "+sql);

              executor.execute(new MyTask(prop,resultSet.getString("dbname")
                  ,resultSet.getString("tablespace")+"."+resultSet.getString("table")
                  ,sql,resultSet.getString("id"),null));

              threadPoolStatus();
            }
            else{

              Util util = new Util();
              String getEndValueSql = "select max("+resultSet.getString("updateField")+") from "
                                    +resultSet.getString("tablespace")+"."+resultSet.getString("table")+" where "
                                    +resultSet.getString("updateField")+" > ";

              if("num".equals(resultSet.getString("updateFieldType")))
              {
                getEndValueSql = getEndValueSql + resultSet.getLong("fieldBeginValue");
              }
              else if("time".equals(resultSet.getString("updateFieldType")))
              {
                getEndValueSql = getEndValueSql + " to_date('"+resultSet.getString("fieldBeginValue")
                                                +"','YYYY-MM-DD HH24:MI:SS')";
              }
              else if("timestamp".equals(resultSet.getString("updateFieldType")))
              {
                getEndValueSql = getEndValueSql + " to_timestamp('"+resultSet.getString("fieldBeginValue")
                    +"','YYYY-MM-DD HH24:MI:SS')";
              }
              else
              {
                getEndValueSql = getEndValueSql + "'"+resultSet.getString("fieldBeginValue")+"'";
              }

              LOG.info("getEndValueSql: "+getEndValueSql);

              String endValue = util.getEndValue(resultSet.getString("dbname"),getEndValueSql);

              if(null != endValue)
              {
                if("timestamp".equals(resultSet.getString("updateFieldType")) || "time".equals(resultSet.getString("updateFieldType")))
                {
                  endValue = endValue.substring(0,19);
                }

                String updateSql = "update "+prop.getProperty("etldb")+"."
                    +prop.getProperty("etltable")
                    +" set fieldEndValue = '"+endValue+"'";
                if(null == resultSet.getString("fieldInitValue") || "".equals(resultSet.getString("fieldInitValue")))
                {
                  updateSql = updateSql+",fieldInitValue = '"+resultSet.getString("fieldBeginValue")+"'";
                }

                updateSql = updateSql + " where id = " + resultSet.getString("id");

                util.update("mysql",updateSql);
              }
              else
              {
                LOG.info("endValue is null,id: "+resultSet.getString("id")+" etl process cancel !!!");
                continue;
              }

              if("num".equals(resultSet.getString("updateFieldType")))
              {
                sql = sql + " and " + resultSet.getString("updateField") + " >= '"
                    + resultSet.getLong("fieldBeginValue")
                    + "' and " + resultSet.getString("updateField") + " < " + endValue ;
              }
              else if("time".equals(resultSet.getString("updateFieldType")))
              {
                sql = sql + " and " + resultSet.getString("updateField") + " >= to_date('"
                    + resultSet.getString("fieldBeginValue") +"','YYYY-MM-DD HH24:MI:SS')"
                    + " and " + resultSet.getString("updateField") + " < to_date('"
                    + endValue + "','YYYY-MM-DD HH24:MI:SS')";
              }
              else if("timestamp".equals(resultSet.getString("updateFieldType")))
              {
                sql = sql + " and " + resultSet.getString("updateField") + " >= to_timestamp('"
                    + resultSet.getString("fieldBeginValue") +"','YYYY-MM-DD HH24:MI:SS')"
                    + " and " + resultSet.getString("updateField") + " < to_timestamp('"
                    + endValue + "','YYYY-MM-DD HH24:MI:SS')";
              }
              else{
                sql = sql + " and " + resultSet.getString("updateField") + " >= '" + resultSet
                    .getString("fieldBeginValue")
                    + "' and " + resultSet.getString("updateField") + " < '" + endValue + "'";
              }
              LOG.info("sql: "+sql);

              executor.execute(new MyTask(prop,resultSet.getString("dbname")
                  ,resultSet.getString("tablespace")+"."+resultSet.getString("table")
                  ,sql,resultSet.getString("id"),endValue));

              threadPoolStatus();

            }
          }
        }


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


  /**
   * @param fullFile 配置文件路径
   * @return Properties对象
   */
  public static Properties loadPropertiesFile(String fullFile) {
    if (null == fullFile || fullFile.equals("")){
      throw new IllegalArgumentException("Properties file path can not be null" + fullFile);
    }
    InputStream inputStream = null;
    Properties p =null;
    try {
      inputStream = new FileInputStream(new File(fullFile));
      p = new Properties();
      p.load(inputStream);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (null != inputStream){
          inputStream.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return p;
  }




}


