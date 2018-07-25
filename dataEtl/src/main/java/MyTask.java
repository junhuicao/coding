import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import javafx.scene.input.DataFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(MyTask.class);

  private Properties prop;
  private String db;
  private String table;
  private String sql;
  private Map<String, String> etlInfoMap;
  private LinkedBlockingQueue<RowBatchResult> rsBatchQueue;
  private ConcurrentHashMap<String,HashMap<String,String>> etlTaskInfoMap;
  private String taskId;

  public MyTask(Map<String, String> etlInfoMap
      , LinkedBlockingQueue<RowBatchResult> rsBatchQueue
      ,ConcurrentHashMap<String,HashMap<String,String>> etlTaskInfoMap
      ,String taskId)
  {
    this.prop = DataEtl.prop;
    this.db = etlInfoMap.get("dbname");
    this.table = etlInfoMap.get("table");
    this.etlInfoMap = etlInfoMap;
    this.rsBatchQueue = rsBatchQueue;
    this.etlTaskInfoMap = etlTaskInfoMap;
    this.taskId = taskId;
  }


  public void run() {

    LOG.info("正在执行生产者task： " + taskId + "_" + db + "_" + table);

    this.sql = getExecuteSql();

    if (null != sql) {
      LOG.info("执行sql： " + sql);

      Connection connection = null;
      PreparedStatement ps = null;
      Connection conn = null;
      Statement st = null;
      ResultSet resultSet = null;
      String updateSql = null;
      String filePath = null;
      Long rowCount = 0L;

      //这里的()表示保存匹配的结果,需要替换的特殊字符：
      // 特别是下面的字符如果是字段值的一部分时，必须前缀一个反斜杠：反斜杠本身，换行符，回车，以及当前分隔符
      String colDelimiter = prop.getProperty("colDelimiter");
      String delimsRegex = "([\\\\\r\n" + colDelimiter + "])";
      boolean isHandleDelims = false;
      if (null != etlInfoMap.get("handleDelims") && etlInfoMap.get("handleDelims").equals("1")) {
        isHandleDelims = true;
      }
      LOG.info("---> " + table + " is handle delims: " + isHandleDelims);

      try {
        connection = Util.getConnect(db);

        ps = connection.prepareStatement(sql);
        resultSet = ps.executeQuery();

        //Util.getColumnInfo(resultSet.getMetaData());

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        boolean blobFlag = false;
        List<Integer> blobIndexList = new ArrayList<Integer>();

        for (int i = 1; i <= columnCount; i++) {
          if (resultSetMetaData.getColumnTypeName(i).equals("BLOB"))
          {
            blobFlag = true;
            blobIndexList.add(i - 1);
          }
        }

        if (blobFlag) {
          ArrayList<Object[]> rsArrayList = new ArrayList<Object[]>();
          RowBatchResult rowBatchResult = null;

          while (resultSet.next()) {
            rowCount = rowCount+1;
            Object[] rowObj = new Object[columnCount];
            for (int i = 1; i <= columnCount; i++) {
              rowObj[i - 1] = resultSet.getObject(i);
            }
            rsArrayList.add(rowObj);
            if (rowCount%Integer.parseInt(prop.getProperty("blobRowBatch")) == 0) {
              rowBatchResult = new RowBatchResult();
              rowBatchResult.setId(etlInfoMap.get("id"));
              rowBatchResult.setTaskId(taskId);
              rowBatchResult
                  .setBatchId((int)(rowCount / Integer.parseInt(prop.getProperty("blobRowBatch"))));
              rowBatchResult.setBlobIndexList(blobIndexList);
              rowBatchResult.setDb(etlInfoMap.get("dbname"));
              rowBatchResult.setTableSpace(etlInfoMap.get("tablespace"));
              rowBatchResult.setTable(etlInfoMap.get("table"));
              rowBatchResult.setRowBatchRs(rsArrayList);
              DataEtl.executor.execute(
                  new BlobHandleTask(
                      rowBatchResult
                      , prop.getProperty("colDelimiter")
                      , isHandleDelims
                      , delimsRegex
                      , rsBatchQueue
                      ,etlTaskInfoMap
                      ,taskId));
              rsArrayList = new ArrayList<Object[]>();

              setBatchInfo(rowBatchResult.getBatchId()+"","0");
            }
          }
          if (rsArrayList.size() > 0) {
            rowBatchResult = new RowBatchResult();
            rowBatchResult.setId(etlInfoMap.get("id"));
            rowBatchResult.setTaskId(taskId);
            rowBatchResult
                .setBatchId((int)(rowCount / Integer.parseInt(prop.getProperty("blobRowBatch"))+1));
            rowBatchResult.setBlobIndexList(blobIndexList);
            rowBatchResult.setDb(etlInfoMap.get("dbname"));
            rowBatchResult.setTableSpace(etlInfoMap.get("tablespace"));
            rowBatchResult.setTable(etlInfoMap.get("table"));
            rowBatchResult.setRowBatchRs(rsArrayList);
            DataEtl.executor.execute(
                new BlobHandleTask(
                    rowBatchResult
                    , prop.getProperty("colDelimiter")
                    , isHandleDelims
                    , delimsRegex
                    , rsBatchQueue
                    ,etlTaskInfoMap
                    ,taskId));
            setBatchInfo(rowBatchResult.getBatchId()+"","0");
          }

          LOG.info("row count is: "+rowCount);
          etlTaskInfoMap.get(taskId).put("rowCnt",rowCount+"");

        } else {
          StringBuffer buff = new StringBuffer();
          while (resultSet.next()) {
            rowCount++;
            for (int i = 1; i <= columnCount; i++) {
              String postfix = colDelimiter;
              if (i == columnCount) {
                postfix = "\r\n";
              }

              if (resultSet.getString(i) != null) {
                if (isHandleDelims) {
                  buff.append(resultSet.getString(i).replaceAll(delimsRegex, "\\\\$1") + postfix);
                } else {
                  buff.append(resultSet.getString(i) + postfix);
                }
              } else {
                buff.append(postfix);
              }
            }

            if (rowCount % Integer.parseInt(prop.getProperty("buffRowCnt")) == 0) {
              int batchId = (int) (rowCount / Integer.parseInt(prop.getProperty("buffRowCnt")));
              setBatchInfo(batchId+"","0");
              setRowBatchandPutQueue(buff,batchId);
              buff = new StringBuffer();

            }
          }

          if (buff.length() > 0) {
            int batchId = (int) (rowCount / Integer.parseInt(prop.getProperty("buffRowCnt")) + 1);
            setBatchInfo(batchId+"","0");
            setRowBatchandPutQueue(buff,batchId);
          }

          etlTaskInfoMap.get(taskId).put("rowCnt",rowCount+"");

          resultSet.close();
          ps.close();
          connection.close();
          LOG.info(table + " this read row count: " + rowCount);

        }
      } catch (Exception e) {
        etlTaskInfoMap.get(taskId).put("status","fail");
        if(etlTaskInfoMap.containsKey(taskId+"_"+"batch"))
        {
          LOG.error("任务："+taskId+"_"+db+"_"+table+"出错，但已经提交了部分batch！！！",e);
        }
        else
        {
          LOG.error("任务："+taskId+"_"+db+"_"+table+"出错，还未提交batch，移除此任务！！！",e);
          new Util().updateTaskInfo(etlTaskInfoMap.get(taskId),taskId);
          etlTaskInfoMap.remove(taskId);
        }

      } finally {
        if (resultSet != null) {
          try {
            resultSet.close();
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
        if (ps != null) {
          try {
            ps.close();
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
        if (st != null) {
          try {
            st.close();
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
        if (conn != null) {
          try {
            conn.close();
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }

      }
    }
  }

  public void setRowBatchandPutQueue(StringBuffer buff,int batchId)
  {
    RowBatchResult rowBatchResult = new RowBatchResult();
    rowBatchResult.setId(etlInfoMap.get("id"));
    rowBatchResult.setTaskId(taskId);
    rowBatchResult
        .setBatchId(batchId);
    rowBatchResult.setDb(etlInfoMap.get("dbname"));
    rowBatchResult.setTableSpace(etlInfoMap.get("tablespace"));
    rowBatchResult.setTable(etlInfoMap.get("table"));
    rowBatchResult.setRowBatchResult2Str(buff.delete(buff.length()-2,buff.length()));
    try {
      rsBatchQueue.put(rowBatchResult);
      LOG.info("etl: "+taskId+" 批量行结果集第"+rowBatchResult.getBatchId()+"批添加队列成功。");
      if(etlTaskInfoMap.get(taskId).containsKey("addBatchCnt"))
      {
        etlTaskInfoMap.get(taskId).put(
            "addBatchCnt",
            ""+(Integer.parseInt(etlTaskInfoMap.get(taskId).get("addBatchCnt"))+1)
            );
      }
      else
      {
        etlTaskInfoMap.get(taskId).put("addBatchCnt",1+"");
      }
    } catch (InterruptedException e) {
      etlTaskInfoMap.get(taskId).put("status","fail");
      LOG.error("etl: "+taskId+" 批量行结果集第"+rowBatchResult.getBatchId()+"批添加队列失败！！！", e);
    }
  }

  public void setBatchInfo(String batchId,String batchStatus)
  {
    /*往etlTaskInfoMap中添加batch信息的map,0=运行，1=成功，2=失败*/
    String batchKey = taskId+"_batch";
    if(etlTaskInfoMap.containsKey(batchKey))
    {
      etlTaskInfoMap.get(batchKey).put(batchId,batchStatus);
    }
    else
    {
      HashMap<String,String> bachInfoMap = new HashMap<String,String>();
      bachInfoMap.put(batchId,batchStatus);
      etlTaskInfoMap.put(taskId+"_batch",bachInfoMap);
    }
  }

  public String getExecuteSql() {

    String sql = "";

    LOG.info(etlInfoMap.get("isAll"));

    if ((null == etlInfoMap.get("updateSql")) || (etlInfoMap.get("updateSql").length() == 0)) {
      sql = "select * from " + etlInfoMap.get("tablespace")
          + "." + etlInfoMap.get("table")
          + " where 1=1 ";
    } else {
      sql = etlInfoMap.get("updateSql");
    }

    if ("fill" == etlInfoMap.get("etlType")) {
      sql = sql + " and fieldBeginValue >= '" + etlInfoMap.get("fieldBeginValue")
          + "' and fieldBeginValue < '" + etlInfoMap.get("fieldEndValue") + "'";
    } else if ("his" == etlInfoMap.get("etlType")) {
      sql = sql + " and fieldInitValue < '" + etlInfoMap.get("fieldInitValue") + "'";
    } else {
      if ("all".equals(etlInfoMap.get("isAll"))) {

        LOG.info("sql: " + sql);
      } else {
        if ("T+1".equals(etlInfoMap.get("updateType")) && "time"
            .equals(etlInfoMap.get("updateFieldType")))
        {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
          Date toDay = new Date();
          Date yesterDay = new Date(toDay.getTime() - 86400000L);
          String toDayStr = sdf.format(toDay) + " 00:00:00";
          String yesterDayStr = sdf.format(yesterDay) + " 00:00:00";

          String updateSql = "update " + prop.getProperty("etldb") + "."
              + prop.getProperty("etltable")
              + " set fieldEndValue = '" + toDayStr + "',fieldBeginVAlue = '" + yesterDayStr + "'";
          if (null == etlInfoMap.get("fieldInitValue") || ""
              .equals(etlInfoMap.get("fieldInitValue"))) {
            updateSql = updateSql + ",fieldInitValue = '" + yesterDayStr + "'";
          }

          updateSql = updateSql + " where id = " + etlInfoMap.get("id");

          new Util().update("mysql", updateSql);

          sql = sql + " and " + etlInfoMap.get("updateField") + " >= to_date('"
              + yesterDayStr + "','YYYY-MM-DD HH24:MI:SS') and " + etlInfoMap.get("updateField")
              + " < to_date('" + toDayStr + "','YYYY-MM-DD HH24:MI:SS')";

          LOG.info("sql: " + sql);
        } else {

          Util util = new Util();
          String getEndValueSql = "select max(" + etlInfoMap.get("updateField") + ") from "
              + etlInfoMap.get("tablespace") + "." + etlInfoMap.get("table") + " where "
              + etlInfoMap.get("updateField") + " > ";

          if ("num".equals(etlInfoMap.get("updateFieldType"))) {
            getEndValueSql = getEndValueSql + Long.parseLong(etlInfoMap.get("fieldBeginValue"));
          } else if ("time".equals(etlInfoMap.get("updateFieldType"))) {
            getEndValueSql = getEndValueSql + " to_date('" + etlInfoMap.get("fieldBeginValue")
                + "','YYYY-MM-DD HH24:MI:SS')";
          } else if ("timestamp".equals(etlInfoMap.get("updateFieldType"))) {
            getEndValueSql = getEndValueSql + " to_timestamp('" + etlInfoMap.get("fieldBeginValue")
                + "','YYYY-MM-DD HH24:MI:SS')";
          } else {
            getEndValueSql = getEndValueSql + "'" + etlInfoMap.get("fieldBeginValue") + "'";
          }

          LOG.info("getEndValueSql: " + getEndValueSql);

          String endValue = util.getEndValue(etlInfoMap.get("dbname"), getEndValueSql);

          if (null != endValue) {
            if ("timestamp".equals(etlInfoMap.get("updateFieldType")) || "time"
                .equals(etlInfoMap.get("updateFieldType"))) {
              endValue = endValue.substring(0, 19);
            }

            String updateSql = "update " + prop.getProperty("etldb") + "."
                + prop.getProperty("etltable")
                + " set fieldEndValue = '" + endValue + "'";
            if (null == etlInfoMap.get("fieldInitValue") || ""
                .equals(etlInfoMap.get("fieldInitValue"))) {
              updateSql =
                  updateSql + ",fieldInitValue = '" + etlInfoMap.get("fieldBeginValue") + "'";
            }

            updateSql = updateSql + " where id = " + etlInfoMap.get("id");

            util.update("mysql", updateSql);

            if ("num".equals(etlInfoMap.get("updateFieldType"))) {
              sql = sql + " and " + etlInfoMap.get("updateField") + " >= "
                  + Long.parseLong(etlInfoMap.get("fieldBeginValue"))
                  + " and " + etlInfoMap.get("updateField") + " < " + endValue;
            } else if ("time".equals(etlInfoMap.get("updateFieldType"))) {
              sql = sql + " and " + etlInfoMap.get("updateField") + " >= to_date('"
                  + etlInfoMap.get("fieldBeginValue") + "','YYYY-MM-DD HH24:MI:SS')"
                  + " and " + etlInfoMap.get("updateField") + " < to_date('"
                  + endValue + "','YYYY-MM-DD HH24:MI:SS')";
            } else if ("timestamp".equals(etlInfoMap.get("updateFieldType"))) {
              sql = sql + " and " + etlInfoMap.get("updateField") + " >= to_timestamp('"
                  + etlInfoMap.get("fieldBeginValue") + "','YYYY-MM-DD HH24:MI:SS')"
                  + " and " + etlInfoMap.get("updateField") + " < to_timestamp('"
                  + endValue + "','YYYY-MM-DD HH24:MI:SS')";
            } else {
              sql = sql + " and " + etlInfoMap.get("updateField") + " >= '"
                  + etlInfoMap.get("fieldBeginValue")
                  + "' and " + etlInfoMap.get("updateField") + " < '" + endValue + "'";
            }
            LOG.info("sql: " + sql);
            etlTaskInfoMap.get(taskId).put("endValue",endValue);
          } else {
            LOG.info("没有增量记录,id为" + etlInfoMap.get("id") + "的etl进程取消 !!!");
            etlTaskInfoMap.remove(taskId);
            sql = null;
          }
        }
      }
    }
    return sql;
  }
}
