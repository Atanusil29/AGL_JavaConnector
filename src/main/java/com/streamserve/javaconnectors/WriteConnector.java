package com.streamserve.javaconnectors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;

import au.com.astral.exstream.OTCSConnect;
import streamserve.connector.StrsConfigVals;
import streamserve.connector.StrsConnectable;
import streamserve.connector.StrsServiceable;

/**
 * Implementation of stub output connector. At the end of the job writes message to the StreamServer log. 
 */
public class WriteConnector implements StrsConnectable {

	private static final String LOG_PREFIX = "Java Object : Template Generation : ";
	
	private static final String PROPERTYNAME_CONFIG_FOLDER = "Configuration Folder";
	private static final String PROPERTYNAME_FILENAME = "Filename";
	private static final String PROPERTYNAME_TRADEID = "Trade ID";
	private static final String PROPERTYNAME_EXTENTSION = "Extension";
	private static final String PROPERTYNAME_TRACKERID = "Tracker ID";
	private static final String PROPERTYNAME_TIMESTAMP = "Timestamp";
	private static final String PROPERTYNAME_COMMODITYCODE = "Commodity code";
	private static final String PROPERTYNAME_COMMODITYID = "Commodity ID";
	private static final String PROPERTYNAME_MESSAGEID = "Message ID";
	private static final String PROPERTYNAME_MESSAGESOURCE = "Message Source";
	private static final String PROPERTYNAME_DOCUMENTTYPE = "Document Type";
	private static final String PROPERTYNAME_DOCUMENTSTATUS = "Document Status";
	private static final String PROPERTYNAME_JOBSTATUS = "Job status";
	private static final String PROPERTYNAME_VALIDATION_VAR = "Validation variables";
	private static final String PROPERTYNAME_VALIDATION_STATUS = "Validation status";
		
	private String ConfigFileFolder;
	private String csFilename;
	private String csTradeID;
	private String csExtension;
	private String csTrackerID;
	private String csTimestamp;
	private String csCommodityCode;
	private String csCommodityID;
	private String csMessageID;
	private String csMessageSource;
	private String csDocumentType;
	private String csDocumentStatus;
	private String csJobStatus;
	private String csValidationVars;
	private String csValidationStatus;

	private StrsServiceable m_service;
	private ByteArrayOutputStream m_outStream;
	
	public void readConfigVals(StrsConfigVals configVals) {
		
		//Get the values of the specified keys.
		String csconf = configVals.getValue(PROPERTYNAME_CONFIG_FOLDER);
		if (csconf.length() > 0) {
			ConfigFileFolder = csconf;
		}
		String csfname = configVals.getValue(PROPERTYNAME_FILENAME);
		if (csfname.length() > 0) {
			csFilename = csfname;
		}
		String cstrid = configVals.getValue(PROPERTYNAME_TRADEID);
		if (cstrid.length() > 0) {
			csTradeID = cstrid;
		}
		String ext = configVals.getValue(PROPERTYNAME_EXTENTSION);
		if (ext.length() > 0) {
			csExtension = ext;
		}
		String cstrckid = configVals.getValue(PROPERTYNAME_TRACKERID);
		if (cstrckid.length() > 0) {
			csTrackerID = cstrckid;
		}
		String csts = configVals.getValue(PROPERTYNAME_TIMESTAMP);
		if (csts.length() > 0) {
			csTimestamp = csts;
		}
		String ccd = configVals.getValue(PROPERTYNAME_COMMODITYCODE);
		if (ccd.length() > 0) {
			csCommodityCode = ccd;
		}
		String cid = configVals.getValue(PROPERTYNAME_COMMODITYID);
		if (cid.length() > 0) {
			csCommodityID = cid;
		}
		String csmid = configVals.getValue(PROPERTYNAME_MESSAGEID);
		if (csmid.length() > 0) {
			csMessageID = csmid;
		}
		String csm = configVals.getValue(PROPERTYNAME_MESSAGESOURCE);
		if (csm.length() > 0) {
			csMessageSource = csm;
		}	
		String csdt = configVals.getValue(PROPERTYNAME_DOCUMENTTYPE);
		if (csdt.length() > 0) {
			csDocumentType = csdt;
		}	
		String csds = configVals.getValue(PROPERTYNAME_DOCUMENTSTATUS);
		if (csds.length() > 0) {
			csDocumentStatus = csds;
		}			
		String csjs = configVals.getValue(PROPERTYNAME_JOBSTATUS);
		if (csjs.length() > 0) {
			csJobStatus = csjs;
		}
		String csv = configVals.getValue(PROPERTYNAME_VALIDATION_VAR);
		if (csv.length() > 0) {
			csValidationVars = csv;
		}
		String csvs = configVals.getValue(PROPERTYNAME_VALIDATION_STATUS);
		if (csvs.length() > 0) {
			csValidationStatus = csvs;
		}

		//Gets the StrsService class to use when logging from a Connector class
		if (m_service == null) {
			m_service = configVals.getStrsService();
		}
	}
	
	/**
	 * StrsConnectable implementation
	 * 
	 * 	The StreamServer calls this method directly after the connector has been created.
	 *  Use this method to initialize resources according to the connector properties set in Design Center.
	 *  The properties are passed in the ConfigVals object and can be accessed with getValue method.
	*/
	public boolean strsoStartJob(StrsConfigVals configVals) throws RemoteException {
		
	//On connector startup, see if connector settings are entered
		try {
			readConfigVals(configVals);
				m_service.writeMsg(StrsServiceable.MSG_INFO, 3, "Java Output connector : " + "Start up successful...");
				
		} catch (Exception e) {			
				m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, "Java Output connector : " + ": " + e.getLocalizedMessage());
			return false;
		}		
		return true;
	}
	
	/**
	 * StrsConnectable implementation
	 * 
	 * 	The StreamServer calls this method each time it starts processing output data.
	 *  can be used to initialize resources according to connector properties set in Design Center.
	 *  The properties are passed in the ConfigVals object and can be accessed with getValue method.
	*/
	public boolean strsoOpen(StrsConfigVals configVals) throws RemoteException {
		
		//Read config values and open output stream
		readConfigVals(configVals);
		m_outStream = new ByteArrayOutputStream();
		
		return true;
	}

	
	/**
	 * StrsConnectable implementation
	 * 
	 *  This method is called between a pair of strsoOpen() and strsoClose() calls. It can be called several times or only once,
	 *  depending on the amount of data to be written. Each strsoWrite() call provides buffered output data.
	 */
	public boolean strsoWrite(byte[] bytes) throws RemoteException {
		
		//Write document from Communication Server to ByteArrayOutputStream
		try {
			m_outStream.write(bytes);
		} catch (IOException e) {
			m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, "Java Output connector : " + ": " + "Error during writing of data to output connector");
			return false;
		}
		return true;
	}

	/**
	 * StrsConnectable implementation
	 * 
	 *  The StreamServer calls this method at the end of the Process, Document or Job. 
	 *  use this method to performed the final delivery.
	 *  If the connector supports runtime properties, these are passed in the ConfigVals object. 
	 */
	public boolean strsoClose(StrsConfigVals configVals) throws RemoteException {
		
		//Close outputstream
		try {
			m_outStream.close();
		} catch (IOException e) {
			m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, "Java Output connector : " + ": " + "Error during closing process of writing of data to output connector");
		}
		
		//Variable initialisation
		final String webReportNameStatus = "AccelMessageAzureServiceBus_WR";
		final String webReportNameFetchDocID = "AccelFetchDestinationID_WR";
		String actionType = "";
		String documentID = "";
		String status = "";
		String success = "";
		String Status= "GENERATE INITIATED";
		String WM_Document_Type  = "";
		String errorCode= "";
		String errorType= "";
		String errorMessage= "";
		String error = "0";
		String errorString= "";
		
		String msg= "";
		
		//Read connector settings
		readConfigVals(configVals);	
		
		//Initialize web report values
		String[] webReportParameters = new String[] { "BoKey", "DocType" ,"DocName", "DocStatus" };
		String[] webReportparValues = new String[] { csTradeID, csDocumentType, csFilename, csDocumentStatus };
		
		//Initialize category update parameters
		String[] categoryValues = new String[] { csTrackerID, csTimestamp };
		
		//Determine WM_Document_Type category
		if (csDocumentType.compareTo("Invoice") == 0) WM_Document_Type  = "Invoice";
		else if (csDocumentType.compareTo("Confirmation") == 0) WM_Document_Type = "Trade Confirmation";
		
		//Create OTCSConnect object
		OTCSConnect contentServerObject = new OTCSConnect();
		m_service.writeMsg(StrsServiceable.MSG_INFO, 4, "Java Object : Template Generation : Class created, reading configuration file");
		
		//Read configuration file
		if (error.compareTo("0") == 0) {
			error = contentServerObject.readConfigFile(ConfigFileFolder + "\\config.json");
			if (error.compareTo("0") == 0) {
				m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + "Configuration file read");
			} else {
				msg = "Java error : Failed to read configuration file";
				m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + msg);
				errorMessage = msg;
			}
		}

		//Content Server get authentication ticket
		if (error.compareTo("0") == 0) {
			error = contentServerObject.getOTCSTicketEncrypted();
			if (error.compareTo("0") == 0) {
				m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + "Content Server authentication ticket Received");
			}else {
				msg = "Java error : Failed to retrieve Content Server authentication tickets";
				m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + msg);
				errorMessage = msg;
			}
		}
		
		//Check if previous Exstream error occured
		if (csJobStatus.compareTo("0") == 0) {
			msg = "Java Object : An Exstream error occured, skipping document upload";
			m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, msg);
			errorMessage = msg;
			error = msg;
		}
		
		//Check if variables were validated
		if (csValidationStatus.compareTo("true") == 0) {
			m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, "Java Object : A JSON validation failed, skipping document upload");
			errorMessage = "JSON Validation unsuccessful : Variables : " + csValidationVars;
			error = "JSON Validation unsuccessful";
		}
		
		//Get web report from Content Server to retrieve ActionType and NodeID values
		if (error.compareTo("0") == 0) {
			error = contentServerObject.getWebReport(webReportNameFetchDocID, webReportParameters, webReportparValues);
			if (error.compareTo("0") == 0) {
				m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + webReportNameFetchDocID + " web report retrieved");
			}
			else {
				msg = "Java error : Failed to retrieve " + webReportNameFetchDocID + " web report";
				m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + msg);
				errorMessage = msg;
			}
		}
		
		//Parse web report response into variables
		if (error.compareTo("0") == 0) {
			status = contentServerObject.parseWebReportParameter("Status");
			if (status.compareTo("Success") == 0) {
				actionType = contentServerObject.parseWebReportParameter("ActionType");
				documentID = contentServerObject.parseWebReportParameter("NodeID");
				m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + "Content Server write action type: " + actionType + " Document ID : " + documentID);				
			} else {
				msg = "Trade Workspace not found in Content server";
				m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, LOG_PREFIX + msg);
				errorMessage = "Java error : " + msg;
			}
		}
		
		//Upload document to Content Server
		if (error.compareTo("0") == 0){
			
			//Add new document workflow
			if(actionType.contentEquals("AddNewDoc")) {
				documentID = contentServerObject.writeDocumentConnector(m_outStream, csFilename, csExtension, documentID, categoryValues, WM_Document_Type);
				if(documentID.contains("writeDocument")==false) {
					m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + "New document added to Content Server");
				} else {
					msg = "Writing document to Content Server failed";
					m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, LOG_PREFIX + msg);
					errorMessage = "Java error : " + msg;
				}
			
			//Add a new version workflow
			} else if (actionType.contentEquals("AddVersion")){
				String versionNr = contentServerObject.writeDocumentConnector(m_outStream,csExtension , documentID, true);
				if(versionNr.contains("writeDocument")==false) {
					m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + "New document version added to Content Server");
					
					//Content Server Category update
					if (error.compareTo("0") == 0) {
						error = contentServerObject.categoryUpdate(categoryValues, documentID);
						if (error.compareTo("0") == 0) {
							m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + "Categories updated successfully");
						} else {
							msg = "Failed to update categories in Content Server";
							m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, LOG_PREFIX + msg);
							errorMessage = "Java error : " + msg;
						}
					}
				} else {
					m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, LOG_PREFIX + "Writing document version to Content Server failed");
					errorMessage = "Java error : Writing document version to Content Server failed";
					error = versionNr;
				}
			} else error = "Invalid action type returned by Web report";
			
			if(documentID.contains("writeDocument")) {
				error = documentID;
			}
		}
		
		//Exstream final status message
		if (error.compareTo("0") != 0) {	
			m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, LOG_PREFIX + "Process Failed : " + error);
			errorString = " with errors";
		} else {
			m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + "Successful, updating status...");
		}
			
		//Set error information for Content Server Status Update
		if (error.compareTo("0") == 0) {
			success="true"; errorCode="250"; 
			if (csDocumentType.compareTo("Invoice") == 0) 			Status= "INVOICE CREATED";
			else if (csDocumentType.compareTo("Confirmation") == 0) Status= "CONFIRM CREATED";
		} else if (error.compareTo("An Exstream error occured") == 0) {
			success = "false"; errorCode="500"; errorType="EXSTR_GEN_ERROR"; Status= "GENERATE FAILED";
		} else if (error.compareTo("JSON Validation unsuccessful") == 0) {
			success = "false"; errorCode="550"; errorType="EXSTR_GEN_ERROR"; Status= "GENERATE FAILED";
		}else {
			success = "false"; errorCode="400"; errorType="EXSTR_GEN_ERROR"; Status= "GENERATE FAILED";
		}
		
		//Initialize status update parameters
		String[] statusUpdateParameters = new String[] { "BoKey","DocType","Status","CommodityID","CommodityCode","MessageId","MessageSource","success","ErrorCode","ErrorType","ErrorMessage"};
		String[] statusUpdateValues = new String[] { csTradeID,csDocumentType, Status,csCommodityID,csCommodityCode,csMessageID,csMessageSource,success,errorCode,errorType,errorMessage};
		
		//Content Server Status Update
		error = contentServerObject.getWebReport(webReportNameStatus, statusUpdateParameters, statusUpdateValues);
		if (error.compareTo("0") == 0) {
			m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + "Content Server status updated");
		} else {
			m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, LOG_PREFIX + "An error occurred while updating the status in Content Server");
		}
		
		//Final status message
		if (error.compareTo("0") != 0) {	
			m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, LOG_PREFIX + "Status update Failed : " + error);
		} else {
			m_service.writeMsg(StrsServiceable.MSG_INFO, 4, LOG_PREFIX + "Completed" + errorString);
		}
						
		return true;
	}

	/**
	 * StrsConnectable implementation
	 * 
	 *  The StreamServer calls this method when all data has been delivered by the output connector and before
	 *  the connector is removed. Use this method to release the resources used by the connector.
	 */
	public boolean strsoEndJob() throws RemoteException {
		return true;
	}
}