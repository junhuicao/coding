import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import javafx.scene.input.DataFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Util.class);

  private Properties prop;
  private String db;
  private String table;
  private String sql;
  private String id;
  private String endValue;

  public MyTask(Properties prop,String db,String table,String sql,String id,String endValue){
    this.prop = prop;
    this.db = db;
    this.table = table;
    this.sql = sql;
    this.id = id;
    this.endValue = endValue;
  }


  public void run() {
    LOG.info("正在执行task： " +id+"_"+ db +"_"+ table);
    LOG.info("执行sql： " + sql);

    Connection connection = null;
    PreparedStatement ps = null;
    FileOutputStream outputStream = null;
    BufferedOutputStream bufferedOutputStream = null;
    Connection conn = null;
    Statement st = null;
    ResultSet resultSet = null;
    String updateSql = null;
    String filePath = null;

    try {
      connection = Util.getConnect(db);

      ps = connection.prepareStatement(sql);
      resultSet = ps.executeQuery();

      //Util.getColumnInfo(resultSet.getMetaData());
      int columnCount = resultSet.getMetaData().getColumnCount();
      StringBuffer buff = new StringBuffer();
      filePath = prop.getProperty("tmpFileDir")+table.replace("\"","").replace(".","_")
                                  +"_"+new SimpleDateFormat("yyyyMMdd").format(new Date())+Util.getSuffix(prop.getProperty("interval"));
      outputStream = new FileOutputStream(filePath);
      bufferedOutputStream = new BufferedOutputStream(outputStream);

      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; i++) {
          String postfix = "\001";
          if(i==columnCount)
          {
            postfix = "\r\n";
          }

          if( resultSet.getMetaData().getColumnTypeName(i).equals("BLOB"))
          {
            if(resultSet.getBlob(i) != null && resultSet.getBlob(i).length() > 0)
              buff.append(Util.byte2hex(resultSet.getBlob(i).getBinaryStream())+postfix);
            else
              buff.append(postfix);
          }

          else {
            if (resultSet.getString(i) != null)
            {
              buff.append(resultSet.getString(i) + postfix);
            }
            else
            {
              buff.append(postfix);
            }
          }

        }

        bufferedOutputStream.write(buff.toString().getBytes());
        buff.setLength(0);

      }

      resultSet.close();
      ps.close();
      connection.close();
      bufferedOutputStream.close();
      outputStream.close();

      LOG.info("Begining upload file to ftp.");
      Util.upload(Util.Ftpconnect(prop.getProperty("ftpPath")
          ,prop.getProperty("ftpAddr")
          ,21
          ,prop.getProperty("ftpUser")
          ,prop.getProperty("ftpPwd"))
          ,new File(filePath));

      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
      String dt = df.format(new Date());// new Date()为获取当前系统时间

      updateSql="update " + prop.getProperty("etldb")+"."+ prop.getProperty("etltable") +" set executeStatus = 'success', updateTime = '"+dt+"'";
      if(null != endValue) updateSql = updateSql + ", fieldBeginValue = '"+endValue+"'";
      updateSql = updateSql + " where id = "+id;

      LOG.info("update etlTable sql: "+updateSql);

      conn = Util.getConnect(prop.getProperty("etlConn"));
      st = conn.createStatement();
      st.executeUpdate(updateSql);

      st.close();
      conn.close();

      LOG.info("task: " + db+"."+table + " 执行完毕。");
    } catch (Exception e) {
      LOG.error("etl task execute error !!!",e);

      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
      String dt = df.format(new Date());// new Date()为获取当前系统时间

      updateSql="update " +prop.getProperty("etldb")+"."+prop.getProperty("etltable") +" set executeStatus = 'error',updateTime = '"+dt+"'";
      if(null != endValue) updateSql = updateSql + ", fieldEndValue = '"+endValue+"'";
      updateSql = updateSql + " where id = "+id;

      LOG.info("update etlTable sql: "+updateSql);

      try {
        conn = Util.getConnect(prop.getProperty("etlConn"));
        st = conn.createStatement();
        st.executeUpdate(updateSql);

        st.close();
        conn.close();
      } catch (Exception e1) {
        LOG.error("etl task handle exeption error !!! ",e1);
      }
      LOG.info("task: " + db+"."+table + " 执行完毕。");

    } finally {
      if(resultSet != null)
      {
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
      if(bufferedOutputStream != null)
      {
        try {
          bufferedOutputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if(outputStream != null)
      {
        try {
          outputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if(st != null)
      {
        try {
          st.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if(conn != null)
      {
        try {
          conn.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }

    }
  }
}
