import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import oracle.sql.BLOB;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.BinaryColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

  private static final Logger LOG = LoggerFactory.getLogger(Util.class);

  public static Map<String,DruidDataSource> dsMap = new HashMap<String,DruidDataSource>();
  public static DruidDataSource dataSource=null;

    /**
   * 读取数据库配置文件
   */
  public String readDbConfFile(String fileName) {
  File file = new File(fileName);
  BufferedReader reader = null;
  String jsonStr = "";
  try {
    reader = new BufferedReader(new FileReader(file));
    String tempString = null;

    // 一次读入一行，直到读入null为文件结束
    while ((tempString = reader.readLine()) != null) {
      jsonStr = jsonStr+tempString;
    }
    reader.close();

  } catch (IOException e) {
    e.printStackTrace();
  } finally {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e1) {
      }
    }
  }
    LOG.info(jsonStr);
  return jsonStr;
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


  public static String getDateStr()
  {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
    String dt = df.format(new Date());// new Date()为获取当前系统时间
    return dt;
  }

  public JSONObject jsonStr2jsonObject(String jsonStr) {

    JSONObject jsonObject = JSONObject.parseObject(jsonStr);


    for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
      System.out.println(entry.getKey() + ":" + entry.getValue());
    }

    return jsonObject;

  }

  public static String getEndValue(String db,String sql) {

    LOG.info("getEndValue()->db: "+db);
    LOG.info("getEndValue()->sql: "+sql);

    Connection connection = null;
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String rs = null;
    try {


      connection = getConnect(db);

      ps = connection.prepareStatement(sql);
      resultSet = ps.executeQuery();

      if(resultSet.next()) {
        rs = resultSet.getString(1);
      }

      resultSet.close();
      ps.close();
      connection.close();
    } catch (Exception e) {
      e.printStackTrace();
    }finally {
      if(resultSet != null)
      {
        try {
          resultSet.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
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
    return rs;
  }

  public  String updateTaskInfo(HashMap<String,String> taskInfoMap,String taskId)
  {
    String insertSql = "insert into "+DataEtl.prop.getProperty("etldb")+"."+DataEtl.prop.getProperty("etlTaskInfoTable")
        +" (etlid,taskid,taskName,rowCnt,begintime,endtime,status,remark,beginValue,endValue,updatetime) "
        + "values("
        + taskInfoMap.get("id")
        + ",'" +taskId+"'"
        + ",'" +taskInfoMap.get("taskName")+"'"
        + ",'" +taskInfoMap.get("rowCnt")+"'"
        + ",'" +taskInfoMap.get("beginTime")+"'"
        + ",'" +taskInfoMap.get("endTime")+"'"
        + ",'" +taskInfoMap.get("status")+"'"
        + ",'" +taskInfoMap.get("remark")+"'"
        + ",'" +taskInfoMap.get("beginValue")+"'"
        + ",'" +taskInfoMap.get("endValue")+"'"
        + ",'" +getDateStr()+"'"
        + ")";
    LOG.info("Insert etlTaskInfo Sql: "+insertSql);
    return update(DataEtl.prop.getProperty("etlConn"),insertSql);
  }

  public  String update(String db,String sql) {

    LOG.info("update()->db: "+db);
    LOG.info("update()->sql: "+sql);

    Connection connection = null;
    Statement st=null;
    String rs = null;
    try {


      connection = getConnect(db);
      st = connection.createStatement();
      int flag = st.executeUpdate(sql);
      System.out.println("update: "+flag);
      if(flag==1) rs = "success";
      else rs = "error";

      st.close();
      connection.close();
    } catch (Exception e) {
      e.printStackTrace();
    }finally {
      if(st != null) {
        try {
          st.close();
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
    return rs;
  }

  public static void GetDbConnect(String jsonStr) {



    //String path = System.getProperty("user.dir")+File.separator+"server.prop";
    //Properties prop = loadPropertiesFile(path);

    JSONObject jsonObject = JSONObject.parseObject(jsonStr);


    for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
      System.out.println(entry.getKey() + " : " + entry.getValue());
      JSONObject tmpJson = JSONObject.parseObject(entry.getValue().toString());
      try{

        dataSource=new DruidDataSource();
        //设置连接参数
        dataSource.setUrl(tmpJson.getString("url"));
        dataSource.setDriverClassName(tmpJson.getString("driverClassName"));
        dataSource.setUsername(tmpJson.getString("usr"));
        dataSource.setPassword(tmpJson.getString("pwd"));
        //配置初始化大小、最小、最大
        dataSource.setInitialSize(tmpJson.getIntValue("initialSize"));
        dataSource.setMinIdle(tmpJson.getIntValue("minIdle"));
        dataSource.setMaxActive(tmpJson.getIntValue("maxActive"));
        //连接泄漏监测
        dataSource.setRemoveAbandoned(true);
        dataSource.setRemoveAbandonedTimeout(30);
        //配置获取连接等待超时的时间
        dataSource.setMaxWait(tmpJson.getIntValue("maxWait"));
        //配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
        dataSource.setTimeBetweenEvictionRunsMillis(tmpJson.getIntValue("timeBetweenEvictionRunsMillis"));
        //防止过期
        dataSource.setValidationQuery(tmpJson.getString("validationQuery"));
        dataSource.setTestWhileIdle(tmpJson.getBooleanValue("testWhileIdle"));
        dataSource.setTestOnBorrow(tmpJson.getBooleanValue("testOnBorrow"));
        dsMap.put(entry.getKey(),dataSource);
      }catch(Exception e){
        throw e;
      }
    }

  }

  public static String byte2hex(InputStream input)  {

    StringBuilder builder = new StringBuilder();
    try {

      byte[] byteBuff = new byte[1024 * 1024];
      int byteRead = 0;
      while ((byteRead = input.read(byteBuff, 0, 1024 * 1024)) != -1) {

        builder.append(bytesToHexString(byteBuff, byteRead));
      }

      input.close();

    }catch (Exception e)
    {
      e.printStackTrace();
    }
    finally {
      if(input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return  builder.toString();
  }

  public static void writeFile()  {

    try {
      File file = new File("D:\\2");
      FileInputStream input = new FileInputStream(file);
      InputStreamReader reader = new InputStreamReader(input);
      FileOutputStream outputStream = new FileOutputStream("D:\\2.png");
      //outputStream = new FileOutputStream("D:\\test");
      BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

      char[] charBuff = new char[1024];
      int byteRead = 0;

      while ((byteRead = reader.read(charBuff)) != -1) {

        bufferedOutputStream.write(hexStringToByte(new String(charBuff)));
      }




      input.close();
      bufferedOutputStream.close();
      outputStream.close();
    }catch (Exception e)
    {
      e.printStackTrace();
    }
  }


  public static String bytesToHexString(byte[] src,int byteRead){
    StringBuilder stringBuilder = new StringBuilder();
    if (src == null || src.length <= 0) {
      return null;
    }
    for (int i = 0; i < byteRead; i++) {
      int v = src[i] & 0xFF;
      String hv = Integer.toHexString(v);


      if (hv.length() < 2) {
        stringBuilder.append(0);
      }
      stringBuilder.append(hv);
    }
    return stringBuilder.toString();
  }

  public static byte[] hexStringToByte(String hex) {
    int len = (hex.length() / 2);
    byte[] result = new byte[len];
    char[] achar = hex.toCharArray();
    for (int i = 0; i < len; i++) {
      int pos = i * 2;
      result[i] = (byte) (toByte(achar[pos]) << 4 | toByte(achar[pos + 1]));
    }
    return result;
  }

  private static byte toByte(char c) {
    byte b = (byte) "0123456789abcdef".indexOf(c);
    return b;
  }

    /**
     * 取得已经构造生成的数据库连接
     * @return 返回数据库连接对象
     * @throws Exception
     */
  public static Connection getConnect(String db) throws Exception{
    Connection con=null;
    try {
      con=dsMap.get(db).getConnection();
    } catch (Exception e) {
      throw e;
    }
    return con;
  }

  public static void write2blob() {

    Connection conn = null;
    PreparedStatement pt = null;
    ResultSet rset = null;

    try {
      conn = Util.getConnect("ogg");
      pt = conn.prepareStatement("insert into TCLOUD.T_OGG (id,\"zp\") values(14,empty_blob()) ");
      pt.execute();
      pt.close();

      pt = conn.prepareStatement("select \"zp\" from TCLOUD.T_OGG where id= 14 for update");
      rset = pt.executeQuery();

      File file = new File("D:\\gc思维导图.png");
      FileInputStream fin = new FileInputStream(file);

      if (rset.next()) {
        try {
          BLOB oracleblob = (oracle.sql.BLOB) rset.getBlob(1);
          OutputStream out = oracleblob.getBinaryOutputStream();
          BufferedOutputStream output = new BufferedOutputStream(out);
          BufferedInputStream input = new BufferedInputStream(fin);
          byte[] buff = new byte[2048]; //用做文件写入的缓冲
          int bytesRead;
          while (-1 != (bytesRead = input.read(buff, 0, buff.length))) {
            output.write(buff, 0, bytesRead);
          }
          fin.close();
          out.flush();
          output.close();


        } catch (Exception e) {
          e.printStackTrace();
        }


      }
      pt.executeUpdate();//修改
      rset.close();
      pt.close();
      conn.commit();//提交
      conn.close();

  } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }finally {
      if(rset != null)
      {
        try {
          rset.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if(pt != null)
      {
        try {
          pt.close();
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

  /**
   *
   * @param path 上传到ftp服务器哪个路径下
   * @param addr 地址
   * @param port 端口号
   * @param username 用户名
   * @param
   * password 密码
   * @return
   * @throws Exception
   */
  public static  FTPClient Ftpconnect(String path,String addr,int port,String username,String password) {
    boolean result = false;
    FTPClient ftp = new FTPClient();
    try {
      int reply;
      ftp.connect(addr, port);
      ftp.login(username, password);
      ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
      reply = ftp.getReplyCode();
      if (!FTPReply.isPositiveCompletion(reply)) {
        ftp.disconnect();
        LOG.error("ftp connect failed !!!");
        return null;
      }
      LOG.info("--------------------->>>> "+path);
      String [] dirs = path.split("/");
      for(String dirss:dirs)
      {
        ftp.makeDirectory(dirss);
        ftp.changeWorkingDirectory(dirss);
      }


      result = true;

    }catch (Exception e){
      LOG.error("Ftp connect error !!!",e);
      result = false;
    }

    LOG.info("Ftp connect ststus: " + result + ". ftp addr:" + addr + ". ftp path: " + path);
    return ftp;
  }

  /**
   *
   * @param file 上传的文件或文件夹
   * @throws Exception
   */
  public static boolean upload(FTPClient ftp,File file) {

    boolean rs = false;

    try {
      if (file.isDirectory()) {
        ftp.makeDirectory(file.getName());
        ftp.changeWorkingDirectory(file.getName());
        String[] files = file.list();
        for (int i = 0; i < files.length; i++) {
          File file1 = new File(file.getPath() + "\\" + files[i]);
          if (file1.isDirectory()) {
            upload(ftp, file1);
            ftp.changeToParentDirectory();
          } else {
            File file2 = new File(file.getPath() + "\\" + files[i]);
            FileInputStream input = new FileInputStream(file2);
            ftp.storeFile(file2.getName(), input);
            input.close();
          }
        }
      } else {
        File file2 = new File(file.getPath());
        LOG.info("Ftp---->  upload from "+file.getAbsolutePath() + " .fileName: "+ file.getName());
        FileInputStream input = new FileInputStream(file2);
        ftp.storeFile(file2.getName(), input);
        input.close();
      }
      rs = true;
    }
    catch (Exception e){
      LOG.error("Ftp uplod file error !!!",e);
    }
    return  rs;
  }

  public static String getSuffix(String interval)
  {
    String suffix = null;
    SimpleDateFormat daySdf = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat Sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    LOG.info(daySdf.format(new Date()));
    try {
      suffix = (new Date().getTime()-Sdf.parse(daySdf.format(new Date())+" 00:00:00").getTime())/(1000*600)+"";
      LOG.info("suffix: "+suffix);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return suffix;
  }

  public static void getColumnInfo(ResultSetMetaData data) throws SQLException {


      for (int i = 1; i <= data.getColumnCount(); i++) {
        // 获得所有列的数目及实际列数
        int columnCount = data.getColumnCount();
        // 获得指定列的列名
        String columnName = data.getColumnName(i);
// 获得指定列的列值
        int columnType = data.getColumnType(i);
// 获得指定列的数据类型名
        String columnTypeName = data.getColumnTypeName(i);
// 所在的Catalog名字
        String catalogName = data.getCatalogName(i);
// 对应数据类型的类
        String columnClassName = data.getColumnClassName(i);
// 在数据库中类型的最大字符个数
        int columnDisplaySize = data.getColumnDisplaySize(i);
// 默认的列的标题
        String columnLabel = data.getColumnLabel(i);
// 获得列的模式
        String schemaName = data.getSchemaName(i);
// 某列类型的精确度(类型的长度)
        int precision = data.getPrecision(i);
// 小数点后的位数
        int scale = data.getScale(i);
// 获取某列对应的表名
        String tableName = data.getTableName(i);
// 是否自动递增
        boolean isAutoInctement = data.isAutoIncrement(i);
// 在数据库中是否为货币型
        boolean isCurrency = data.isCurrency(i);
// 是否为空
        int isNullable = data.isNullable(i);
// 是否为只读
        boolean isReadOnly = data.isReadOnly(i);
// 能否出现在where中
        boolean isSearchable = data.isSearchable(i);
        System.out.println(columnCount);
        System.out.println("获得列" + i + "的字段名称:" + columnName);
        System.out.println("获得列" + i + "的类型,返回SqlType中的编号:"+ columnType);
        System.out.println("获得列" + i + "的数据类型名:" + columnTypeName);
        System.out.println("获得列" + i + "所在的Catalog名字:"+ catalogName);
        System.out.println("获得列" + i + "对应数据类型的类:"+ columnClassName);
        System.out.println("获得列" + i + "在数据库中类型的最大字符个数:"+ columnDisplaySize);
        System.out.println("获得列" + i + "的默认的列的标题:" + columnLabel);
        System.out.println("获得列" + i + "的模式:" + schemaName);
        System.out.println("获得列" + i + "类型的精确度(类型的长度):" + precision);
        System.out.println("获得列" + i + "小数点后的位数:" + scale);
        System.out.println("获得列" + i + "对应的表名:" + tableName);
        System.out.println("获得列" + i + "是否自动递增:" + isAutoInctement);
        System.out.println("获得列" + i + "在数据库中是否为货币型:" + isCurrency);
        System.out.println("获得列" + i + "是否为空:" + isNullable);
        System.out.println("获得列" + i + "是否为只读:" + isReadOnly);
        System.out.println("获得列" + i + "能否出现在where中:"+ isSearchable);
      }


}

  public static void taskStatusCheck(ConcurrentHashMap<String,HashMap<String,String>> etlTaskInfoMap
                              ,String taskId,String taskStatus,String batchId,String batchStatus,String remark)
  {

    if(null != taskStatus) {
      etlTaskInfoMap.get(taskId).put("status", taskStatus);
    }
    etlTaskInfoMap.get(taskId+"_batch").put(batchId,batchStatus);
    etlTaskInfoMap.get(taskId).put("remark",etlTaskInfoMap.get(taskId).get("remark")
        +"\r\n批次"+batchId+": "+remark);
    if(etlTaskInfoMap.get(taskId).containsKey("rowCnt") && !etlTaskInfoMap.get(taskId+"_batch").containsValue("0"))
    {
      LOG.info("taskId: "+taskId+", batchId: "+batchId+", taskName: "
          +etlTaskInfoMap.get(taskId).get("taskName")+". 所有批次已经运行完成，移除该任务！！！");
      etlTaskInfoMap.get(taskId).put("endTime",Util.getDateStr());
      new Util().updateTaskInfo(etlTaskInfoMap.get(taskId),taskId);
      etlTaskInfoMap.remove(taskId);
      etlTaskInfoMap.remove(taskId+"_batch");
    }
  }

  /**
   * 用java Api读写orc
   * @throws IOException
   */
  public static void writeOrc() throws IOException {
    Configuration conf = new Configuration();
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int,z:binary>");
    Writer writer = OrcFile.createWriter(new Path("D:\\my-file.orc"),
        OrcFile.writerOptions(conf)
            .setSchema(schema));

    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector x = (LongColumnVector) batch.cols[0];
    LongColumnVector y = (LongColumnVector) batch.cols[1];
    BytesColumnVector z = (BytesColumnVector) batch.cols[2];
    for(int r=0; r < 2; ++r) {
      int row = batch.size++;
      x.vector[row] = r;
      y.vector[row] = r * 3;
      //z.vector[row] = "1433233".getBytes();
      z.setVal(row,("14\t\r\n\00133233"+r).getBytes());
      // If the batch is full, write it out and start over.
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();

    Reader reader = OrcFile.createReader(new Path("D:\\my-file.orc"),
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch1 = reader.getSchema().createRowBatch();
    while (rows.nextBatch(batch1)) {
      for(int r=0; r < batch1.size; ++r) {
        for(int i=0; i< batch1.numCols;i++)
        {
          System.out.println(((LongColumnVector)batch1.cols[1]).vector.toString());
        }
      }
    }
    rows.close();

  }

}


