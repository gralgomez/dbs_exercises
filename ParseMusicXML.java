package musicDB;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


import org.xml.sax.*;
import org.w3c.dom.*;

import javax.xml.parsers.*;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ParseMusicXML {

	private NodeList	itemList = null;
	private NodeList	trackList = null;
	private NodeList	discList = null;
	private XPath		xpath = null;
	private Node        actItem = null;
	private int			actItemNum = -1;
	private Node        actTrack = null;
	private int			actTrackNum = -1;
	private Node        actDisc = null;
	private int			actDiscNum = -1;
	private static Log logger = LogFactory.getLog(ParseMusicXML.class);

    /**
     *  Konstruktor fuer ParseMusic XML
     *  @param fName Dateiname der XML-Datei, die geparst werden soll
     *  @throws IOException
     *  
     */
	public ParseMusicXML(String fName) throws IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		try{
			DocumentBuilder builder  = factory.newDocumentBuilder();
			Document        document = builder.parse(new File(fName));
			
			xpath = XPathFactory.newInstance().newXPath();
			/* 
			 * Erzeugen einer Knotenliste aller Item Elemente aus dem Dokument,
			 * Benutzen von XPath::evaluate(...)
			 */
			itemList = (NodeList) xpath.evaluate("/ItemSearchResponse/Items/Item",document,XPathConstants.NODESET);
			logger.debug("ItemListLength: " + itemList.getLength());
			if(!next())
				throw new IOException("no Items found");
		} catch( SAXException sxe ) {
			Exception e = ( sxe.getException() != null ) ? sxe.getException() : sxe;
			logger.error(e.getMessage());
			throw new IOException(e.getMessage());
		} catch(ParserConfigurationException e ) {
			logger.error(e.getMessage());
			throw new IOException(e.getMessage());
		} catch(XPathExpressionException e) {
			logger.error(e.getMessage());
			throw new IOException(e.getMessage());
		}
	}

	/**
	 * Prueft, ob noch ein CD-Eintrag in der Liste gespeichert ist und
	 * setzt die Variable actItem. Initialisiert die Daten, um auf die
	 * erste Disc und den ersten Track zugreifen zu koennen.
	 * @return true, wenn noch ein Item mit einer Disc und Tracks existiert
	 * 		   false, sonst
	 * 
	 */
	public boolean next() {
		boolean rc = false;
		while(!rc){
			actItem = itemList.item(++actItemNum);
			if(actItem!=null){
				logger.debug("ActItemNum: " + actItemNum + " ItemListLength: "  + itemList.getLength());
				logger.debug("actItem: "+ actItem);
				rc = true;
				try{
					discList = (NodeList) xpath.evaluate("Tracks/Disc",actItem,XPathConstants.NODESET);
					actDiscNum = -1;
					rc = nextDisc();
				}catch(XPathExpressionException e) {
					logger.error(e.getMessage());
				}
			}else{
				break;
			}
		}
		logger.debug("rc: "+ rc);
		return rc;
	}

	/**
	 * Prueft ob noch eine weitere Disc zu der aktuellen CD gehoert und
	 * setzt die Variable actDisc darauf. Dann werden alle Tracks dieser
	 * Disc in trackList gespeichert. Mit getDiscNumber kann dann darauf
	 * zugegriffen werden. Es wird nexTrack aufgerufen und somit der erste
	 * Track initialisiert. Danach kann auch nextTrack()
	 * benutzt werden, um auf die weiteren Tracks dieser Disc zuzugreifen.
	 * @see ParseMusicXML#nextTrack()
	 * @return true, wenn noch eine Disk mit Tracks existiert
	 * 		   false, sonst
	 * 
	 */
	public boolean nextDisc() {
		boolean rc = false;
		actDisc = discList.item(++actDiscNum);
		logger.debug("ActDiscNum: " + actDiscNum + "DiscListLength: "  + discList.getLength());
		logger.debug("actDisc: "+ actDisc);
		if(actDisc != null){
			rc = true;
			try{
				trackList = (NodeList) xpath.evaluate("Track",actDisc,XPathConstants.NODESET);
				actTrackNum = -1;
				rc = nextTrack();
			}catch(XPathExpressionException e) {
				logger.error(e.getMessage());
				rc = false;
			}
		}
		logger.debug("rc: " + rc);
		return rc;
	}
	
	/**
	 * Prueft, ob noch ein weiterer Track auf der aktuellen Disk vorhanden ist
	 * und setzt die Variable actTrack darauf. Danach kann mit getTrackNumber
	 * und getTrackTitle darauf zugegriffen werden.
	 * @return true, wenn noch ein Track auf der aktuellen Disk existiert
	 * 		   false, sonst
	 */
	public boolean nextTrack() {
		boolean rc = false;
		actTrack = trackList.item(++actTrackNum);
		logger.debug("ActTrackNum: " + actTrackNum + "TrackListLength: "  + trackList.getLength());
		logger.debug("actTrack: "+ actTrack);
		if(actTrack != null){
			rc = true;
		}
		logger.debug("rc: " + rc);
		return rc;
	}

	/**
	 * Wertet den XPath-Ausdruck auf dem aktuellen Item aus
	 * und gibt den Textinhalt des/der Resultknoten(s) zurueck.
	 * - Leerzeichen am Anfang und Ende des Strings sollen geloescht werden (String::trim())
	 * @param xpathStr 
	 * @return String
	 * 
	 */
	private String getStringForXPath(String xpathStr) {
		String rc = null;
		logger.debug("ActItem: " + actItem);
		try{
			rc = xpath.evaluate(xpathStr,actItem);
			rc.trim();
			logger.debug("XPath: "+ xpathStr+" Res: " + rc);
		}catch(XPathExpressionException e){
			e.printStackTrace();
		}
		if(rc!=null && rc.length() == 0){
			logger.error("Error in getStringForXPath: " + xpathStr);
			throw new NullPointerException("getStringForXPath" + xpathStr);
		}
		return rc;
	}
	
	/**
	 * Gibt den ASIN-Code der akutellen CD zurueck.
	 * @return ASIN Code String
	 * 
	 */
	public String getASIN() {
		return getStringForXPath("ASIN");
	}
	
	/**
	 * Gibt den aktuellen Artist oder Author zurueck.
	 * @return Artist/Author String
	 * 
	 */
	public String getArtistOrAuthor() {
		return getStringForXPath("ItemAttributes/Artist|ItemAttributes/Author");
	}

	/**
	 * Liefert das aktuelle Label.
	 * @return Label String
	 * 
	 */
	public String getLabel() {
		return getStringForXPath("ItemAttributes/Label");
	}

	/**
	 * Liefert die Anzahl von Discs.
	 * @return int
	 * 
	 */
	public int getNumDiscs() {
		String val = null;
		val = getStringForXPath("ItemAttributes/NumberOfDiscs");
		if(val == null || val.length() == 0){
			logger.error("Error in getNumDiscs");
			throw new NullPointerException("getNumDiscs");
		}
		return Integer.parseInt(val);
	}

	/**
	 * Liefert den Title der CD.
	 * @return String
	 * 
	 */
	public String getTitle() {
		return getStringForXPath("ItemAttributes/Title");
	}

	/**
	 * Liefert den Preis der CD.
	 * @return float
	 */
	private float getPrice(String xpath) {
		String val = null;
		val = getStringForXPath(xpath);
		float rc = 0;
		if(val == null){
			logger.error("Error in getPrice: " + xpath);
			throw new NullPointerException("getPrice " + xpath);
		}
		try{
			rc = Float.parseFloat(val)/(float)100.0;
		}catch(NumberFormatException e){
			logger.equals(e.getMessage() + xpath);
			throw new NullPointerException(e.getMessage() + xpath);
		}
		return  rc;
	}
	
	/**
	 * Liefert den LowNewPrice.
	 * @return float
	 * 
	 */
	public float getLowNewPrice() {
		return getPrice("OfferSummary/LowestNewPrice/Amount");
	}

	/**
	 * Liefert den LowUsedPrice.
	 * @return float
	 * 
	 */
	public float getLowUsedPrice() {
		return getPrice("OfferSummary/LowestUsedPrice/Amount");
	}

	/**
	 * Liefert den Wert fuer das Attribut Number des Knoten actN.
	 * @param actN Kontextknoten fuer die Suche nach AttributeName "Number"
	 * @return int
	 * 
	 */
	private int getAttributeNumber(Node actN) {
		logger.debug("ActNode: " + actN);
		if(actN == null)
			return -1;
		String num = null;
		num = actN.getAttributes().getNamedItem("Number").getNodeValue();
		logger.debug("Num: " + num);
		if(num==null || num.length()==0){
			logger.error("Error in getAttributeNumber");
			throw new NullPointerException("getAttributeNumber");
		}
		return Integer.parseInt(num);
	}
	
	/**
	 * Liefert die aktuelle Disknummer.
	 * @return int
	 */
	public int getDiscNumber() {
		return getAttributeNumber(actDisc);
	}

	/**
	 * Liefert den aktuellen Tracktitel.
	 * @return String
	 * 
	 */
	public String getTrackTitle() {
		logger.debug("ActTrack: " + actTrack);
		String rc = null;
		if(actTrack != null)
			rc = actTrack.getTextContent();
		if(rc == null || rc.length()==0){
			logger.error("Error in getTrackTitle");
			throw new NullPointerException("getTrackTitle");
		}
		return rc;
	}

	/**
	 * Liefert die aktuelle Tracknummer.
	 * @return int
	 */
	public int getTrackNumber() {
		return getAttributeNumber(actTrack);
	}
	
	/**
	 * Parst die gegebene Datei und gibt die Informationen aus.
	 * @param fileName Name der XML-Datei
	 */
	public static void printXMLMusicContent(String fileName) {
		try {
			ParseMusicXML xml = new ParseMusicXML(fileName);
			do {
				try {
					System.out.println("------------------------------------");
					System.out.println("ASIN: " + xml.getASIN());
					System.out.println("Artist: " + xml.getArtistOrAuthor());
					System.out.println("Title: " + xml.getTitle());
					System.out.println("Label: " + xml.getLabel());
					System.out.println("LowNewPrice: " + xml.getLowNewPrice());
					System.out.println("LowUsedPrice: " + xml.getLowUsedPrice());
					System.out.println("NumDiscs: " + xml.getNumDiscs());
					do {
						System.out.println("\tDiscNumber: " + xml.getDiscNumber());
						do {
							System.out.println("\t\tTrack "+ xml.getTrackNumber() + ": " + xml.getTrackTitle());
						} while(xml.nextTrack());
					} while(xml.nextDisc());
				} catch (NullPointerException e) {
					System.out.println("Error invalid field in " + e.getMessage());
				}
			} while(xml.next());
		} catch(IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * Parst die gegebene Datei und laedt die Informationen in die Datenbank.
	 * - alle XML-Daten, die die DTD verletzen, sollen nicht eingefuegt werden
	 *   - Transaktionsklammer fuer jedes einzelne Item
	 *   - Benutzung von Connection.setAutoCommit(boolean)
	 * @param fileName  spezifizierte XML-Datei
	 * @param dbName    Datenbankname
	 * 
	 * 8 Punkte
	 */
	public static void loadXMLMusicContentIntoDB(String fileName,String dbName){
		/* BEGIN */
		Connection con = null;
		PreparedStatement insert_cd = null;
		PreparedStatement insert_track = null;
		PreparedStatement insert_disc = null;
		ParseMusicXML xml = null;
		String lastASIN = null;
		boolean txn_successful = true;

		/* initialization */
		try {
			xml = new ParseMusicXML(fileName);

			Class.forName("COM.ibm.db2.jdbc.app.DB2Driver").newInstance();
			con = DriverManager.getConnection(String.format("jdbc:db2:%s", dbName));
			con.setAutoCommit(false);
			insert_cd = con.prepareStatement(
					"INSERT INTO CD (ASIN, Artist, Label, Title, LowUsedPrice, LowNewPrice) " +
					"VALUES (?, ?, ?, ?, ?, ?)");
			insert_disc = con.prepareStatement(
					"INSERT INTO Disc (CD_ASIN, Number) " +
					"VALUES (?, ?)");
			insert_track = con.prepareStatement(
					"INSERT INTO Track (CD_ASIN, Disc_Number, Title, Number) " +
					"VALUES (?, ?, ?, ?)");
			
			/* insert loop */
			do {
				try {
					lastASIN = xml.getASIN();

					insert_cd.setString(1, xml.getASIN());
					insert_cd.setString(2, xml.getArtistOrAuthor());
					insert_cd.setString(3, xml.getTitle());
					insert_cd.setString(4, xml.getLabel());
					insert_cd.setFloat(5, xml.getLowUsedPrice());
					insert_cd.setFloat(6, xml.getLowNewPrice());

					insert_cd.executeUpdate();

					insert_disc.setString(1, xml.getASIN());
					insert_track.setString(1, xml.getASIN());
					do {
						insert_disc.setInt(2, xml.getDiscNumber());

						insert_disc.executeUpdate();

						insert_track.setInt(2, xml.getDiscNumber());
						do {
							insert_track.setInt(2, xml.getDiscNumber());
							insert_track.setString(3, xml.getTrackTitle());
							insert_track.setInt(4, xml.getTrackNumber());

							insert_track.executeUpdate();
						} while (xml.nextTrack());
					} while (xml.nextDisc());

					txn_successful = true;
				} catch (SQLException e) {
					logger.error("Error in insert", e);
					txn_successful = false;
				} catch (NullPointerException e) {
					logger.error("Error while reading XML", e);
					txn_successful = false;
				} finally {
					if (txn_successful) {
						con.commit();
						logger.info(String.format("Inserted CD %s successfully", lastASIN));
					} else {
						con.rollback();
						logger.info(String.format("Failed to insert CD %s", lastASIN));
					}
				}
			} while (xml.next());
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException | SQLException
				| IOException | NullPointerException e) {
			logger.fatal("An error occured", e);

			return;
		} finally {
			/* deinitialization and clean up */
			try {
				if (null != con) {
					if (!con.getAutoCommit()) {
						/* roll back any uncommitted transactions */
						con.rollback();
					}

					if (null != insert_cd) {
						insert_cd.close();
					}
					
					if (null != insert_disc) {
						insert_disc.close();
					}
					
					if (null != insert_track) {
						insert_track.close();
					}
					
					con.close();
				}
			} catch (SQLException e) {
				logger.error("An SQL exception occured while cleaning up", e);
			}
		}
		/* END */
	}
	
	/**
	 * Gibt die Hilfe zum korrekten Aufruf aus.
	 */
	static void printUsage(){
		System.err.println("ParseMusicXML [-l filename]|[-p filename]");
		System.err.println("-l filename: laedt die Informationen aus der XML-Datei in die Datenbank: " + Config.getProperty("database","dbprak"));
		System.err.println("-p filename: zeigt die Informationen aus der XML-Datei an");
	}
	
	/**
	 * Parst die spezifizierte XML-Datei und laedt sie in die Datenbank dbPrak (-l)
	 * bzw. gibt die geparsten Informationen aus. 
	 * @param argv definiert die Operation bei -p print, bei -l load.
	 */
	public static void main(String[] argv) {
		if(argv.length == 2){
			if(argv[0].compareTo("-p")==0){
				if(argv[1].indexOf("/")<0){
					printXMLMusicContent(Config.getProperty("paths.dataDir")+argv[1]);
				} else {
					printXMLMusicContent(argv[1]);
				}
			}else if(argv[0].compareTo("-l")==0){
				if(argv[1].indexOf("/")<0){
					loadXMLMusicContentIntoDB(Config.getProperty("paths.dataDir")+argv[1],Config.getProperty("database","dbprak"));
				}else{
					loadXMLMusicContentIntoDB(argv[1],Config.getProperty("database","dbprak"));
				}
			}else{
				printUsage();
			}
		}else{
			printUsage();
		}
	}
}
