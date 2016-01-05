package com.run;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

public class Test {
	  
	public static void main(String[] args) throws Exception {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String str = "tance	0000	88	1429732570181	2015-04-22T18:22:02.000Z	54-E6-FC-1A-F1-A4	00:03:7F:00:00:00	51012200000002";
//		String str = "1425980997100000000035988	100	1007	3736082538	1425980832	1007	3736374031	1813";
		 String[] split = StringUtils.split(str, '\t');
		 /*String out = "";
		 for (String s : split) {
			 out =  out.concat(s).concat("-");
		}
		 System.out.println(out);*/
		String driverClass = "oracle.jdbc.driver.OracleDriver";
		String url = "jdbc:oracle:thin:@192.168.41.14:1521:orcl";
		 
		  
		 JdbcUtil jdbcUtil = new JdbcUtil(driverClass, url, "runvista", "runco");
		 conn = jdbcUtil.getConnection();
		 
		 stmt = conn.prepareStatement("insert into station2(BIGPROTOCOLNAME, PROTOCOL_ID, DATA_SOURCE, DATA_ID, CAPTURE_TIME, AP_MAC, MAC, SERVICE_CODE) values(?, ?, ?, ?, to_date(?,'yyyy-MM-dd HH24:MI:SS'), ?, ?, ?)");
		 /* stmt.setString(1, split[5]);
		  stmt.setString(2, split[6]);
		  
		  Calendar instance = Calendar.getInstance();
		  instance.setTimeInMillis(Long.parseLong(split[4])*1000);
//		  Date captureDate = instance.getTime();
		  
		  java.sql.Date date = new java.sql.Date(instance.getTimeInMillis());
		  stmt.setDate(3, date);
		  stmt.setDate(4, date);*/
		//bigprotocolname
		 System.out.println(split[0]);
		 stmt.setString(1, split[0]);
		 stmt.setLong(2, Long.parseLong(split[1]));
	      //data_source NUMBER
	      stmt.setLong(3, Long.parseLong(split[2]));
	      //data_id 
	      stmt.setString(4, split[3]);
	      
	      //�жϡ�����ʱ�䡱�ֶ��Ƿ�Ϊ��
	      /*if(!StringUtils.isEmpty(split[4]) && !StringUtils.isBlank(split[4])){
	    	  Calendar instance = Calendar.getInstance();
	    	  instance.setTimeInMillis(Long.parseLong(split[4])*1000);
	    	  java.sql.Date date = new java.sql.Date(instance.getTimeInMillis());
	    	  //capture_time
	    	  stmt.setDate(5, date);
	      }else {
	    	  stmt.setDate(5, null);
	      }*/
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
	    	  stmt.setDate(5, null);
	      }
	      //ap_mac
	      stmt.setString(6, split[5]);
	      //mac
	      stmt.setString(7, split[6]);
	      //service_code
	      stmt.setString(8, split[7]);
		  stmt.execute();
		
		  jdbcUtil.release(rs, stmt, conn);
		
		 /*Calendar instance = Calendar.getInstance();
		  instance.setTimeInMillis(Long.parseLong("1425980832")*1000);
//		  Date captureDate = instance.getTime();
		  
		  java.sql.Date date = new java.sql.Date(instance.getTimeInMillis());
		  System.out.println(date);*/
	}
}
