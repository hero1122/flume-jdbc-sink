/**
 * 
 */
package com.run;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liwei
 * 2015年4月15日
 */
public class MyJdbcSink extends AbstractSink implements Configurable {
	  private static final Logger logger = LoggerFactory.getLogger(MyJdbcSink.class);
	  private String driverClass;
	  private String url;
	  private String user;
	  private String password;
	  
	  private Connection conn = null;
	  private PreparedStatement stmt = null;
	  private ResultSet rs = null;
	  
	  private JdbcUtil jdbcUtil;

	  @Override
	  public void configure(Context context) {
	    driverClass = context.getString("driverClass", "defaultValue");
	    url = context.getString("url", "defaultValue");
	    user = context.getString("user", "***");
	    password = context.getString("password", "***");
	    
	    jdbcUtil = new JdbcUtil(driverClass, url, user, password);
	    logger.info("configure.....................");
	    
	    // Process the myProp value (e.g. validation)

	    // Store myProp for later retrieval by process() method
	  }

	  @Override
	  public void start() {
	    // Initialize the connection to the external repository (e.g. HDFS) that
	    // this Sink will forward Events to ..
		//初始化JDBC连接
		  try {
			conn = jdbcUtil.getConnection();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		  logger.info("start............................");
	  }

	  @Override
	  public void stop () {
	    // Disconnect from the external respository and do any
	    // additional cleanup (e.g. releasing resources or nulling-out
	    // field values) ..
		//断开JDBC连接
		 jdbcUtil.release(rs, stmt, conn);
		 logger.info("stop..........................");
	  }

	  @Override
	  public Status process() throws EventDeliveryException {
	    Status status = null;
	    String str = null;

	    // Start transaction
	    Channel ch = getChannel();
	    Transaction txn = ch.getTransaction();
	    txn.begin();
	    try {
	      // This try clause includes whatever Channel operations you want to do

	      Event event = ch.take();
	      /*if(null == event){
	    	  status = Status.BACKOFF;
	    	  return status;
	      }*/

	      // Send the Event to the external repository.
	      // storeSomeData(e);
//	      JdbcUtil jdbcUtil = new JdbcUtil(driverClass, url, user, password);
	      byte[] body = event.getBody();
	      //处理一条bcp数据并且进行入库处理
	      str = new String(body);
	     
	      String[] split = StringUtils.split(str, '\t');
	      
	      stmt = conn.prepareStatement("insert into station(BIGPROTOCOLNAME, PROTOCOL_ID, DATA_SOURCE, DATA_ID, CAPTURE_TIME, AP_MAC, MAC, SERVICE_CODE, AP_MAC_U, MAC_U) values(?, ?, ?, ?, TO_DATE(?,'yyyy-MM-dd HH24:MI:SS'), ?, ?, ?, ?, ?)");
	      //bigprotocolname
	      stmt.setString(1, split[0]);
	      //protocol_id NUMBER
	      stmt.setLong(2, Long.parseLong(split[1]));
	      //data_source NUMBER
	      stmt.setLong(3, Long.parseLong(split[2]));
	      //data_id 
	      stmt.setString(4, split[3]);
	      
	      //判断“捕获时间”字段是否为空
	      String str4 = split[4];
	      if(!StringUtils.isEmpty(str4) && !StringUtils.isBlank(str4)){
	    	 /* Calendar instance = Calendar.getInstance();
	    	  instance.setTimeInMillis(Long.parseLong(split[4])*1000);
	    	  java.sql.Date date = new java.sql.Date(instance.getTimeInMillis());
	    	  //capture_time
	    	  stmt.setDate(5, date);*/
	    	  String[] split2 = StringUtils.split(str4, 'T');
	    	  String substring = split2[1].substring(0, 8);
	    	  String dateStr = split2[0].concat(" ").concat(substring);
	    	  /*DateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
	    	  Date dateTrans = format2.parse(dateStr);
	    	  stmt.setDate(5, new java.sql.Date(dateTrans.getTime()));*/
	    	  stmt.setString(5, dateStr);
	      }else {
//	    	  stmt.setDate(5, null);
	    	  stmt.setString(5, null);
	      }
	      //ap_mac
	      stmt.setString(6, split[5]);
	      //mac
	      stmt.setString(7, split[6]);
	      //service_code
	      stmt.setString(8, split[7]);
	      //AP_MAC_U
	      stmt.setString(9, CommonUtils.convertMac2String(split[5]));
	      //MAC_U
	      stmt.setString(10, CommonUtils.convertMac2String(split[6]));
	      stmt.execute();

	      txn.commit();
	      status = Status.READY;
	    } catch (Throwable t) {
	      txn.rollback();

	      // Log exception, handle individual exceptions as needed
	      if(str!=null){
	    	  logger.info("出错记录："+str);
	      }
	      status = Status.BACKOFF;

	      // re-throw all Errors
	      if (t instanceof Error) {
	        throw (Error)t;
	      }
	    } finally {
	    	//回收prepareStatement对象
	    	if(stmt!=null){
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				stmt = null;
			}
	    	
	      txn.close();
	    }
	    return status;
	  }
	  
	}
