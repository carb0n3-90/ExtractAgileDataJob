package com.x.agile.batchjob.extract;

import java.io.BufferedWriter;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.agile.api.APIException;
import com.agile.api.AgileSessionFactory;
import com.agile.api.ChangeConstants;
import com.agile.api.IAgileSession;
import com.agile.api.IAttachmentFile;
import com.agile.api.ICell;
import com.agile.api.IDataObject;
import com.agile.api.IFolder;
import com.agile.api.IItem;
import com.agile.api.IQuery;
import com.agile.api.IRow;
import com.agile.api.ITable;
import com.agile.api.ITwoWayIterator;

public class ExtractAgileData {
	IAgileSession session = null;
	public static Properties prop;
	final static Logger logger = Logger.getLogger(ExtractAgileData.class);
	static String DELIMITER = "^";
	public static String timeStamp = "";
	String dataFileLoc = null;

	public void init() throws IOException, APIException {

		loadPropertyFile();
		this.session = getAgileSession();
		DELIMITER = prop.getProperty("TXT_FILE_DELIMITER");
		Calendar calobj = Calendar.getInstance();
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
		timeStamp = df.format(calobj.getTime());
		dataFileLoc = prop.getProperty("BASE_PATH_FOR_DATA_FILES") + timeStamp + "/";
		new File(dataFileLoc).mkdirs();

	}

	private void loadPropertyFile() throws FileNotFoundException, IOException {
		prop = new Properties();
		FileInputStream file = null;
		String propFileName = "./config.properties";
		try {
			file = new FileInputStream(propFileName);
			prop.load(file);
			logger.info("config File initialized");
		} catch (IOException e) {
			throw e;
		} finally {
			if (file != null)
				file.close();
		}

	}

	public IAgileSession getAgileSession() throws APIException {
		HashMap<Integer, String> params = new HashMap<Integer, String>();
		params.put(AgileSessionFactory.USERNAME, prop.getProperty("AGL_USER"));
		params.put(AgileSessionFactory.PASSWORD, prop.getProperty("AGL_PWD"));
		params.put(AgileSessionFactory.URL, prop.getProperty("AGL_URL"));

		IAgileSession session = AgileSessionFactory.createSessionEx(params);

		logger.info("Connected to Agile!!!");

		return session;
	}

	public void getAgileSearchResults() throws APIException {
		IFolder folder = (IFolder) session.getObject(IFolder.OBJECT_TYPE, "/" + prop.getProperty("AGL_SEARCH_FOLDER"));
		IQuery query = (IQuery) folder.getChild(prop.getProperty("AGL_SEARCH_NAME"));
		ITable results = query.execute();
		logger.info("Search result count:" + results.size());
		ITwoWayIterator itr = results.getTableIterator();
		while (itr.hasNext()) {
			IRow row = (IRow) itr.next();
			IDataObject aglObj = row.getReferent();
			logger.info(aglObj.getAgileClass());
			try {
				logger.info("Extracting data for Object: "+aglObj);
				getObjDtls(aglObj);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		logger.info(prop.getProperty("AGL_SEARCH_NAME") + "executed successfully.");
	}

	public void getObjDtls(IDataObject aglObj) {
		logger.info("Extracting data for - "+aglObj);
		String objClass = "";
		String tabData = "";
		StringBuilder fileName = new StringBuilder(dataFileLoc);
		String extractScope = "";
		try {
			objClass = aglObj.getAgileClass().getSuperClass().getName().replace(" ", "");
			fileName.append(objClass).append("_");
			fileName.append(aglObj.getAgileClass().getName().replace(" ", "")).append("_");
			
			extractScope=prop.getProperty(objClass.toUpperCase()+ "_TABLES_INSCOPE");
			if(StringUtils.isEmpty(extractScope)){
				extractScope = prop.getProperty("DEFAULT_EXTRACT_TABLES_INSCOPE");
			}
			if (extractScope.contains("Details")) {
				tabData = extractDtlsTab(aglObj);
				if (!tabData.isEmpty()) {
					createPopulateFile(fileName+"Details.txt", tabData, aglObj, null);
				}
			}
			ITable[] allTables = aglObj.getTables();
			String tabName = "";
			for (ITable tab : allTables) {
				if (extractScope.contains(tab.getName())) {
					if ("ATTACHMENTS".equalsIgnoreCase(tab.getName())) {
						extractItemAttachmentTab(aglObj, tab, dataFileLoc + "Attachments/");
					} else {
						tabName = tab.getName().replace(" ", "");
						tabData = extractTableDTLS(aglObj, tab);
						if (!tabData.isEmpty()) {
							createPopulateFile(fileName.toString()+tabName+".txt", tabData, aglObj, tab);
						}
					}
				}
			}

		} catch (APIException e) {
			logger.error(e.getMessage(), e);
		}

	}

	private void createPopulateFile(String fileName, String tabData, IDataObject aglObj, ITable tabName) {
		File f = null;
		try {
			f = new File(fileName);
			if (!f.exists()) {
				String tabHeader = getTableHeaders(aglObj, tabName);
				writeInFile(fileName, tabHeader);
			}
			writeInFile(fileName, tabData);
		} catch (APIException e) {
			logger.error(e.getMessage(), e);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}

	}

	public static String getTableHeaders(IDataObject dataObj, ITable tabObj) throws APIException {
		StringBuilder attrNameList = new StringBuilder();
		ICell[] attrCellArry = null;
		if (tabObj == null) {
			attrCellArry = dataObj.getCells();
		} else {
			ITwoWayIterator tabItr = tabObj.getTableIterator();
			if (tabItr.hasNext()) {
				IRow row = (IRow) tabItr.next();
				attrCellArry = row.getCells();
			}
		}
		if (attrCellArry != null) {
			for (ICell cell : attrCellArry) {
				attrNameList.append(cell.getName().replace(DELIMITER,"")).append(DELIMITER);
			}
		}
		return attrNameList.toString();
	}

	private String extractTableDTLS(IDataObject dataObj, ITable tabObj) throws APIException {
		logger.info("Pulling data for "+dataObj+" : "+tabObj.getName());
		StringBuilder attrValList = new StringBuilder();
		String attrVal = "";
		Object valObj = null;
		ITwoWayIterator tabItr = tabObj.getTableIterator();
		IRow rowObj = null;
		IDataObject refObj = null;
		while (tabItr.hasNext()) {
			rowObj = (IRow) tabItr.next();
			attrValList.append(dataObj.getName()).append(DELIMITER);
			ICell[] cellArray = rowObj.getCells();
			for (ICell cellObj : cellArray) {
				valObj = cellObj.getValue();
				attrVal = (valObj == null ? "" : valObj.toString()).replace("\n", "").replace("\r", "")
						.replace(DELIMITER, "");
				attrValList.append(attrVal).append(DELIMITER);
			}
			attrValList.append("\n");
			logger.info("retrieve ref obj details : "+prop.getProperty("REFERENCE_OBJECT_TABLES").contains(tabObj.getName()));
			if(prop.getProperty("REFERENCE_OBJECT_TABLES").contains(tabObj.getName())){
				refObj = (IDataObject)rowObj.getReferent();
				logger.info("Ref Obj : "+refObj);
				if(refObj!=null){
					getObjDtls(refObj);
				}
			}
		}
		return attrValList.toString();
	}

	private static String extractDtlsTab(IDataObject dataObj) throws APIException {
		Object valObj = null;
		String attrVal = null;
		StringBuilder attrValList = new StringBuilder();
		ICell[] attrCellArry = dataObj.getCells();
		for (ICell attrCell : attrCellArry) {
			try{
			valObj = attrCell.getValue();
			}
			catch(APIException e){
				logger.error("Exception while getting value for "+dataObj+": "+attrCell.getName()+"\n"+e.getMessage());
				if("1014".equals(attrCell.getId().toString())){
					if (dataObj  instanceof IItem) {
						valObj = ((IItem)dataObj).getRevision();
					}
				}
			}
			attrVal = (valObj == null ? "" : valObj.toString()).replace("\n", "").replace("\r", "").replace(DELIMITER,
					"");
			attrValList.append(attrVal).append(DELIMITER);
		}
		return attrValList.toString();
	}

	private static void writeInFile(String fileName, String fileData) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)));
		out.println(fileData);
		out.close();
	}

	private void extractItemAttachmentTab(IDataObject dataObj, ITable attachmnetTab, String attPath) {
		try {
			attPath = attPath + dataObj.getAgileClass().getName() + "/";
			copyAttachments(attachmnetTab, attPath + dataObj.getName() + "/");
		} catch (APIException e) {
			logger.error(e.getMessage(), e);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private static void copyAttachments(ITable attachmentTable, String attachmentPath)
			throws APIException, IOException {
		InputStream inStream = null;
		OutputStream outputStream = null;
		try {
			if (attachmentTable.size() > 0) {
				File theFile = new File(attachmentPath);
				theFile.mkdirs();
				String path = theFile.getAbsolutePath();
				Iterator ffItr = attachmentTable.getTableIterator();
				IRow attachRow = null;
				while (ffItr.hasNext()) {
					attachRow = (IRow) ffItr.next();
					inStream = ((IAttachmentFile) attachRow).getFile();
					String fileName = attachRow.getValue(ChangeConstants.ATT_ATTACHMENTS_FILE_NAME).toString();
					File file = new File(path + "/" + fileName);
					outputStream = new FileOutputStream(file);
					IOUtils.copy(inStream, outputStream);
				}
			}
		} catch (APIException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} finally {
			if (inStream != null)
				inStream.close();
			if (outputStream != null)
				outputStream.close();
		}
	}

}