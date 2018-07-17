public class EtlInfoDao {

  private int id;
  private String updatetype;
  private String dbname;
  private String tablespace;
  private String table;
  private String updateSql;
  private String isAll;
  private String updateField;
  private String fieldBeginValue;
  private String fieldEndValue;
  private String fieldInitValue;
  private String updateFieldType;
  private String updateTime;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getUpdatetype() {
    return updatetype;
  }

  public void setUpdatetype(String updatetype) {
    this.updatetype = updatetype;
  }

  public String getDbname() {
    return dbname;
  }

  public void setDbname(String dbname) {
    this.dbname = dbname;
  }

  public String getTablespace() {
    return tablespace;
  }

  public void setTablespace(String tablespace) {
    this.tablespace = tablespace;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getUpdateSql() {
    return updateSql;
  }

  public void setUpdateSql(String updateSql) {
    this.updateSql = updateSql;
  }

  public String getIsAll() {
    return isAll;
  }

  public void setIsAll(String isAll) {
    this.isAll = isAll;
  }

  public String getUpdateField() {
    return updateField;
  }

  public void setUpdateField(String updateField) {
    this.updateField = updateField;
  }

  public String getFieldBeginValue() {
    return fieldBeginValue;
  }

  public void setFieldBeginValue(String fieldBeginValue) {
    this.fieldBeginValue = fieldBeginValue;
  }

  public String getFieldEndValue() {
    return fieldEndValue;
  }

  public void setFieldEndValue(String fieldEndValue) {
    this.fieldEndValue = fieldEndValue;
  }

  public String getFieldInitValue() {
    return fieldInitValue;
  }

  public void setFieldInitValue(String fieldInitValue) {
    this.fieldInitValue = fieldInitValue;
  }

  public String getUpdateFieldType() {
    return updateFieldType;
  }

  public void setUpdateFieldType(String updateFieldType) {
    this.updateFieldType = updateFieldType;
  }

  public String getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
  }
}
