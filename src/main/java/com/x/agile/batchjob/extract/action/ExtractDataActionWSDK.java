package com.x.agile.batchjob.extract.action;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Logger;

import com.agile.api.APIException;
import com.x.agile.batchjob.extract.ExtractAgileData;

public class ExtractDataActionWSDK {
	final static Logger logger = Logger.getLogger(ExtractDataActionWSDK.class);

	public static void main(String[] args) {
		System.out.println("Agile PQM Data Extract is running ...");
		Calendar calobj = Calendar.getInstance();
		DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
		logger.info("Extract Started @ -- " + df.format(calobj.getTime()));
		try {
			ExtractAgileData srcObj = new ExtractAgileData();
			srcObj.init();
			srcObj.getAgileSearchResults();
		}catch (APIException e) {
			logger.error(e.getMessage(), e);
		} 
		catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		logger.info("Extract Completed @ -- " + df.format(Calendar.getInstance().getTime()));
	}

}