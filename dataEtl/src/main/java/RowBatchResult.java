import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class RowBatchResult {
  private String id;
  private String taskId;
  private int batchId;
  private String db;
  private String tableSpace;
  private String table;
  private List<Integer> blobIndexList;
  private ArrayList<Object[]> rowBatchRs;
  private StringBuffer RowBatchResult2Str;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public int getBatchId() {
    return batchId;
  }

  public void setBatchId(int batchId) {
    this.batchId = batchId;
  }

  public String getDb() {
    return db;
  }

  public void setDb(String db) {
    this.db = db;
  }

  public String getTableSpace() {
    return tableSpace;
  }

  public void setTableSpace(String tableSpace) {
    this.tableSpace = tableSpace;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public List<Integer> getBlobIndexList() {
    return blobIndexList;
  }

  public void setBlobIndexList(List<Integer> blobIndexList) {
    this.blobIndexList = blobIndexList;
  }

  public ArrayList<Object[]> getRowBatchRs() {
    return rowBatchRs;
  }

  public void setRowBatchRs(ArrayList<Object[]> rowBatchRs) {
    this.rowBatchRs = rowBatchRs;
  }

  public StringBuffer getRowBatchResult2Str() {
    return RowBatchResult2Str;
  }

  public void setRowBatchResult2Str(StringBuffer rowBatchResult2Str) {
    RowBatchResult2Str = rowBatchResult2Str;
  }

  @Override
  public String toString() {
    return "RowBatchResult{" +
        "id='" + id + '\'' +
        ", taskId='" + taskId + '\'' +
        ", batchId=" + batchId +
        ", db='" + db + '\'' +
        ", tableSpace='" + tableSpace + '\'' +
        ", table='" + table + '\'' +
        ", blobIndexList=" + blobIndexList +
        ", rowBatchRs=" + rowBatchRs +
        ", RowBatchResult2Str=" + RowBatchResult2Str +
        '}';
  }
}
