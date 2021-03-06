import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ToFtpConsumer implements Runnable{

  private static final Logger LOG = LoggerFactory.getLogger(ToFtpConsumer.class);

  private LinkedBlockingQueue<RowBatchResult> rsBatchQueue;
  private Properties prop;
  private ConcurrentHashMap<String,HashMap<String,String>> etlTaskInfoMap;

  public ToFtpConsumer(LinkedBlockingQueue<RowBatchResult> rsBatchQueue
      ,ConcurrentHashMap<String,HashMap<String,String>> etlTaskInfoMap
      ,Properties prop )
  {
    this.rsBatchQueue = rsBatchQueue;
    this.prop = prop;
    this.etlTaskInfoMap = etlTaskInfoMap;
  }

  public void toTmpFile()
  {
    FileOutputStream outputStream = null;
    BufferedOutputStream bufferedOutputStream = null;
    RowBatchResult rowBatchResult = null;

    try {
      rowBatchResult = rsBatchQueue.take();

      LOG.info("更新consumCnt. taskId: "+rowBatchResult.getTaskId()+". batchId: "+rowBatchResult.getBatchId()+". table: "
          +rowBatchResult.getTable());
      if(etlTaskInfoMap.get(
          rowBatchResult.getTaskId())
          .containsKey("consumeCnt"))
      {
        etlTaskInfoMap.get(rowBatchResult.getTaskId())
            .put("consumeCnt"
                ,(Integer.parseInt(
                    etlTaskInfoMap.get(rowBatchResult.getTaskId())
                        .get("consumeCnt"))+1)+""
            );
      }
      else {
        etlTaskInfoMap.get(rowBatchResult.getTaskId()).put("consumeCnt",1+"");
      }

      String dirPath = prop.getProperty("tmpFileDir")+File.separator+new SimpleDateFormat("yyyyMMdd").format(new Date());
      mkdir(dirPath);
      String dayStr = new SimpleDateFormat("yyyyMMdd").format(new Date());
      String fileName = rowBatchResult.getDb()
          +"_"+rowBatchResult.getTableSpace().replace("\"","")
          +"_"+rowBatchResult.getTable().replace("\"","")
          +"_"+dayStr
          +Util.getSuffix(prop.getProperty("600"))
          +"_"+rowBatchResult.getBatchId();

      if(etlTaskInfoMap.get(rowBatchResult.getTaskId()).get("status").equals("fail"))
      {
        LOG.error("任务："+rowBatchResult.getTaskId()+",批次："+rowBatchResult.getBatchId()
            +" table: "+rowBatchResult.getTableSpace()+"_"+rowBatchResult.getTable()+"状态为失败，放弃写入临时文件");

        Util.taskStatusCheck(etlTaskInfoMap,rowBatchResult.getTaskId(),null
            ,rowBatchResult.getBatchId()+"","2","放弃写入临时文件");
        return;
      }
      String filePath = dirPath+File.separator+fileName;
      outputStream = new FileOutputStream(filePath);
      bufferedOutputStream = new BufferedOutputStream(outputStream);
      bufferedOutputStream.write(rowBatchResult.getRowBatchResult2Str().toString().getBytes());

      bufferedOutputStream.close();
      outputStream.close();

      if(etlTaskInfoMap.get(rowBatchResult.getTaskId()).get("status").equals("fail"))
      {
        LOG.error("任务："+rowBatchResult.getTaskId()+",批次："+rowBatchResult.getBatchId()
            +" table: "+rowBatchResult.getTableSpace()+"_"+rowBatchResult.getTable()+"状态为失败，放弃上传Ftp");

        Util.taskStatusCheck(etlTaskInfoMap,rowBatchResult.getTaskId(),null
        ,rowBatchResult.getBatchId()+"","2","放弃上传Ftp");

        return;
      }

      if(!toFtp(filePath,dayStr))
      {
        LOG.error("消费队列上传Ftp失败！！！");
        Util.taskStatusCheck(etlTaskInfoMap,rowBatchResult.getTaskId(),"fail"
        ,rowBatchResult.getBatchId()+"","2","上传Ftp失败");
      }
      else
      {
        LOG.info("消费队列上传Ftp成功！！！");

        String endValue = etlTaskInfoMap.get(rowBatchResult.getTaskId()).get("endValue");
        Util.taskStatusCheck(etlTaskInfoMap,rowBatchResult.getTaskId(),null
            ,rowBatchResult.getBatchId()+"","1","上传Ftp成功");

        String dt = Util.getDateStr();

        String updateSql = "update " + prop.getProperty("etldb") + "." + prop.getProperty("etltable")
            + " set updateTime = '" + dt + "'";
        if (null != endValue && endValue.length()>0) {
          updateSql = updateSql + ", fieldBeginValue = '" + endValue + "'";
        }
        updateSql = updateSql + " where id = " + rowBatchResult.getId();

        LOG.info(" update etlTable sql: " + updateSql);

        new Util().update(prop.getProperty("etlConn"),updateSql);
        LOG.info("task: " + rowBatchResult.getTaskId() + "table: " + rowBatchResult.getTableSpace()
            + "." + rowBatchResult.getTable() + " 执行完毕。");

      }

    } catch (Exception e) {
      LOG.error("消费队列失败！！！", e);
      Util.taskStatusCheck(etlTaskInfoMap,rowBatchResult.getTaskId(),"fail"
                ,rowBatchResult.getBatchId()+"","2","写入临时文件失败");
    }
    finally {
      if(null != bufferedOutputStream)
      {
        try {
          bufferedOutputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if(null != outputStream)
      {
        try {
          outputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

  }

  public void mkdir(String dirPath)
  {
    File dir = new File(dirPath);
    if(!dir.exists())
    {
      dir.mkdirs();
    }
  }

  public boolean toFtp(String filePath,String dayStr)
  {
    File file = new File(filePath);
    LOG.info(" Begining upload "+file.getName()+" file to ftp.");
    return Util.upload(Util.Ftpconnect(prop.getProperty("ftpPath")+"/"+dayStr
        , prop.getProperty("ftpAddr")
        , 21
        , prop.getProperty("ftpUser")
        , prop.getProperty("ftpPwd"))
        , file);
  }

  @Override
  public void run() {
    while (true) {
      toTmpFile();
    }
  }
}
