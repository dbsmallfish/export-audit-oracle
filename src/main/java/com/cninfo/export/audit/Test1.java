package com.cninfo.export.audit;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;



public class Test1 {
	public static void main(String[] args) throws ParseException {

		int i = getDiffSecond("201509221750", "201509221733");
		
		System.out.println(i);
		
		StringBuffer s = new StringBuffer();
	
		System.out.println(s.length());
		
		if(null == s){
			System.out.println("a");
		}else{
			System.out.println("b");
		}
		
				
		

	}

	public static int getDiffSecond(String thisTime, String lastTime)
			throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
		Date d1 = df.parse(thisTime);
		Date d2 = df.parse(lastTime);

		int second = (int) (d1.getTime() - d2.getTime())/ (1000 * 60);
		return second;

	
	}

	public static String getDateAfter1Hour(String str, int minutes)
			throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");

		Date beforeDate = sdf.parse(str);

		Calendar cal = Calendar.getInstance();

		cal.setTime(beforeDate);

		cal.add(Calendar.MINUTE, minutes);

		Date afterDate = cal.getTime();

		String oneHourAfter = sdf.format(afterDate);
		return oneHourAfter;

	}

}
