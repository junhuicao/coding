import java.sql.Blob;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobHandleTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Util.class);

  private String postFix;
  private RowBatchResult rowBatchRs;
  private LinkedBlockingQueue<RowBatchResult> rsBatchQueue;
  private boolean handleDelimiters;
  private String delimsRegex;
  private ConcurrentHashMap<String,HashMap<String,String>> etlTaskInfoMap;
  private String taskId;

  BlobHandleTask(RowBatchResult rowBatchRs, String postFix, boolean handleDelimiters,
      String delimsRegex, LinkedBlockingQueue<RowBatchResult> rsBatchQueue
      ,ConcurrentHashMap<String,HashMap<String,String>> etlTaskInfoMap,String taskId) {
    this.postFix = postFix;
    this.rowBatchRs = rowBatchRs;
    this.rsBatchQueue = rsBatchQueue;
    this.handleDelimiters = handleDelimiters;
    this.delimsRegex = delimsRegex;
    this.etlTaskInfoMap = etlTaskInfoMap;
    this.taskId = taskId;
  }

  @Override
  public void run() {

    if(etlTaskInfoMap.get(taskId).get("status").equals("fail"))
    {
      LOG.error("该任务的状态为失败，本批次"+rowBatchRs.getBatchId()+"进程取消！！！");
      Util.taskStatusCheck(etlTaskInfoMap,taskId,null,rowBatchRs.getBatchId()
          +"","2","放弃生产");
      return;
    }

    StringBuffer strBuff = new StringBuffer();

    for (Object[] row : rowBatchRs.getRowBatchRs()) {
      for (int i = 0; i < row.length; i++) {
        String postfix = postFix;
        if (i == row.length - 1) {
          postfix = "\r\n";
        }

        if (rowBatchRs.getBlobIndexList().contains(i)) {
          try {
            if (null != row[i] &&
                ((Blob) row[i]).length() > 0) {
              strBuff.append(Util.byte2hex(((Blob) row[i]).getBinaryStream()) + postfix);
            } else {
              strBuff.append(postfix);
            }
          } catch (SQLException e) {
            LOG.error("", e);
          }
        } else {
          if (null != row[i]) {
            if (handleDelimiters) {
              strBuff.append(row[i].toString().replaceAll(delimsRegex, "\\\\$1") + postfix);
            } else {
              strBuff.append(row[i].toString() + postfix);
            }
          } else {
            strBuff.append(postfix);
          }
        }
      }
    }

    rowBatchRs.setRowBatchResult2Str(strBuff.delete(strBuff.length() - 2, strBuff.length()));
    try {
      if(etlTaskInfoMap.get(taskId).get("status").equals("fail"))
      {
        LOG.error("该任务的状态为失败，本批次"+rowBatchRs.getBatchId()+"进程取消！！！");
        Util.taskStatusCheck(etlTaskInfoMap,taskId,null,rowBatchRs.getBatchId()
            +"","2","放弃入队");
        return;
      }
      rsBatchQueue.put(rowBatchRs);
      etlTaskInfoMap.get(taskId).put("remark",etlTaskInfoMap.get(taskId).get("remark")
          +"\r\n批次"+rowBatchRs.getBatchId()+":入队成功");
      LOG.info("etl: "+taskId+" 批量行结果集第"+rowBatchRs.getBatchId()+"批添加队列成功。");
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
    } catch (Exception e) {
      LOG.error("etl: "+taskId+" 批量行结果集第"+rowBatchRs.getBatchId()+"批添加队列失败!!!", e);
      Util.taskStatusCheck(etlTaskInfoMap,taskId,"fail"
          ,rowBatchRs.getBatchId()+"","2","入队失败");
    }
  }
}