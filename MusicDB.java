package musicDB;
 
import java.io.PrintWriter;
import java.sql.*;	// JDBC classes

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MusicDB {
	//	A connection (session) with a specific database
	private Connection co 	  = null;
	private static Log logger = LogFactory.getLog(MusicDB.class);

	/**
	 * Konstructor fuer MusicDB
	 * Aufbau der Verbindung zur Datenbank
	 * 
	 * @param dbName Datenbankname
	 */
	public MusicDB(String dbName){
		createDBConnection(dbName);
	}

	/**
	 * Konstructor fuer MusicDB
	 * Schliessen der Verbindung zur Datenbank
	 */
	public void finalize(){
		closeDBConnection();
	}
	
	/**
	 * Verbindung zum Datenbank-Server aufnehmen
	 * @param dbName datenbank name
	 * 
	 * 4 Punkte
	 */
	private void createDBConnection(String dbName) {
		/* BEGIN */
		Connection con = null;
		try {
			Class.forName("COM.ibm.db2.jdbc.app.DB2Driver").newInstance();
			con = DriverManager.getConnection(String.format("jdbc:db2:%s", dbName));
			con.setAutoCommit(true);
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException | SQLException e) {
			logger.fatal("Could not obtain database connection", e);
			System.exit(-1);
		}
		
		this.co = con;
		/* END */
	}
	
	/**
	 * Verbindung zum Datenbank-Server schliessen.
	 * 
	 * 2 Punkte
	 */ 
	private void closeDBConnection() {
		/* BEGIN */
		try {
			if (null != co) {
				this.co.close();
			}
		} catch (SQLException e) {
			logger.error("Closing database connection failed, giving up", e);
		}
		/* END */
	}
	
	/**
	 * Zeigt die Informationen des ResultSets an: Spaltennamen und Werte
	 *  - Zeichenketten sollen in Hochkommata (') stehen
	 *  - die einzelnen Spalten sollen durch Tabs separiert werden (\t)
	 * @param writer PrinterWriter, auf den das Ergebnis geschrieben werden soll
	 * @param rs ResultSet
	 * @return int Anzahl der selektierten Tupel des Ergebnisses
	 * @throws SQLException
	 * 
 	 * 4 Punkte
	 */
	private int printResult(ResultSet rs, PrintWriter writer) throws SQLException {
		int cnt = 0;
		/* BEGIN */
		ResultSetMetaData md = rs.getMetaData();
		if (md.getColumnCount() > 0) {
			writer.print(md.getColumnLabel(1));
			
			for (int i = 2; i <= md.getColumnCount(); i++) {
				writer.printf("\t%s", md.getColumnLabel(i));
			}
			writer.println();
		}
		
		while (rs.next()) {
			cnt++;
			if (md.getColumnCount() > 0) {
				String col_val = rs.getString(1);
				if ((md.getColumnType(1) == java.sql.Types.VARCHAR ||
						md.getColumnType(1) == java.sql.Types.CHAR) &&
						!rs.wasNull()) {
					writer.printf("\'%s\'", col_val);
				} else {
					writer.printf("%s", col_val);
				}
				
				for (int i = 2; i <= md.getColumnCount(); i++) {
					col_val = rs.getString(i);
					if ((md.getColumnType(i) == java.sql.Types.VARCHAR ||
							md.getColumnType(i) == java.sql.Types.CHAR) &&
							!rs.wasNull()) {
						writer.printf("\t\'%s\'", col_val);
					} else {
						writer.printf("\t%s", col_val);
					}
				}
				writer.println();
			}
		}
		/* END */
		return cnt;
	}


	/**
	 * Zeigt alle CDs ohne ihre Tracks an,
	 * d. h. die Informationen zu ASIN ARTIST TITLE LABEL
	 * LOWNEWPRICE LOWUSEDPRICE NUMOFDISC
	 * @param writer PrinterWriter, auf den das Ergebnis geschrieben werden soll
	 * @return int Anzahl der selektierten Tupel des Ergebnisses
	 * 
	 * 3 Punkte
	 */
	public int showAllCDs(PrintWriter writer) {
		int cnt = 0;
		/* BEGIN */
		PreparedStatement query = null;
		ResultSet rs = null;
		try {
			query = co.prepareStatement(
					"SELECT ASIN, ARTIST, TITLE AS CDTITLE, LABEL, " +
					"LOWNEWPRICE AS NEWPRICE, LOWUSEDPRICE AS USEDPRICE, " +
					"DISCCNT " +
					"FROM CD_with_disccnt " +
					"ORDER BY ARTIST, TITLE");
			rs = query.executeQuery();
			cnt = printResult(rs, writer);
		} catch (SQLException e) {
			logger.error("SELECT statement failed", e);
		} finally {
			try {
				if (null != rs) {
					rs.close();
				}
				
				if (null != query) {
					query.close();
				}
			} catch (SQLException e) {
				logger.warn("Closing result set or closing prepared statement failed, giving up", e);
			}
		}
		/* END */
		return cnt;
	}

	/**
	 * Zeigt eine CD anhand ihres ASIN mit allen Discs und allen Tracks an,
	 * d. h. zuerst die Informationen zu ASIN ARTIST TITLE LABEL
	 * LOWNEWPRICE LOWUSEDPRICE NUMOFDISC
	 * und dann DISCNUM:
	 * TRACKNUM TRACKTITLE
	 * @param asinCode zu selektierende CDs
	 * @param writer PrinterWriter, auf den das Ergebnis geschrieben werden soll
	 * @return int Anzahl der selektierten Tupel des Ergebnisses
	 * 
	 * 6 Punkte
	 */
	public int showSingleCD(String asinCode, PrintWriter writer) {
		int cnt = 0;
		/* BEGIN */
		PreparedStatement query_cd = null;
		PreparedStatement query_disc = null;
		PreparedStatement query_track = null;
		ResultSet cd_results = null;
		ResultSet disc_results = null;
		ResultSet track_results = null;
		boolean txn_successful = true;
		try {
			co.setAutoCommit(false);
			query_cd = co.prepareStatement(
					"SELECT ASIN, ARTIST, TITLE AS CDTITLE, LABEL, " +
					"LOWNEWPRICE AS NEWPRICE, LOWUSEDPRICE AS USEDPRICE, " +
					"DISCCNT " +
					"FROM CD_with_disccnt " +
					"WHERE ASIN = ?");
			query_cd.setString(1, asinCode);

			query_disc = co.prepareStatement(
					"SELECT Number " +
					"FROM Disc " +
					"WHERE CD_ASIN = ? " +
					"ORDER BY Number");
			query_disc.setString(1, asinCode);

			query_track = co.prepareStatement(
					"SELECT Number AS TRACKNUM, Title AS TRACKTITLE " +
					"FROM Track " +
					"WHERE CD_ASIN = ? " +
					"AND Disc_Number = ? " +
					"ORDER BY Number");
			query_track.setString(1, asinCode);

			cd_results = query_cd.executeQuery();
			cnt = printResult(cd_results, writer);

			if (cnt > 0) {
				disc_results = query_disc.executeQuery();
				while (disc_results.next()) {
					int discnum = disc_results.getInt(1);
					writer.printf("DiscNum: %d\n", discnum);
				
					query_track.setInt(2, discnum);
					track_results = query_track.executeQuery();
					printResult(track_results, writer);
					track_results.close();
					track_results = null;
				}
			}
		} catch (SQLException e) {
			logger.error("SELECT statement failed", e);
			txn_successful = false;
		} finally {
			try {
				if (!co.getAutoCommit()) {
					if (txn_successful) {
						co.commit();
					} else {
						co.rollback();
					}
					co.setAutoCommit(true);
				}
			} catch (SQLException e) {
				logger.error("Commit failed", e);
			}
			try {
				if (null != track_results) {
					track_results.close();
				}
				
				if (null != disc_results) {
					disc_results.close();
				}
				
				if (null != cd_results) {
					cd_results.close();
				}
				
				if (null != query_track) {
					query_track.close();
				}
				
				if (null != query_disc) {
					query_disc.close();
				}
				
				if (null != query_cd) {
					query_cd.close();
				}
			} catch (SQLException e) {
				logger.warn("Closing result set or closing prepared statement failed, giving up", e);
			}
		}
		/* END */
		return cnt;
	}
	
	/**
	 * Ausgabe des Durchschnittspreises, MIN, MAX aller CD bzgl. LowUsedPrice
	 * und LowNewPrice
	 * @param writer PrinterWriter, auf den das Ergebnis geschrieben werden soll
	 * @return int Anzahl der selektierten Tupel des Ergebnisses
	 * 
	 * 3 Punkte
	 */
	public int showGroupAllPrices(PrintWriter writer) {
		int cnt = 0;
		/* BEGIN */
		PreparedStatement query_prices = null;
		ResultSet price_results = null;
		try {
			query_prices = co.prepareStatement(
					"SELECT " +
					"DECIMAL(AVG(LOWNEWPRICE), 6, 2) AS AVG_NEW, " +
					"DECIMAL(AVG(LOWUSEDPRICE), 6, 2) AS AVG_USED, " +
					"MIN(LOWNEWPRICE) AS MIN_NEW, " +
					"MIN(LOWUSEDPRICE) AS MIN_USED, " +
					"MAX(LOWNEWPRICE) AS MAX_NEW, " +
					"MAX(LOWUSEDPRICE) AS MAX_USED " +
					"FROM CD");

			price_results = query_prices.executeQuery();
			cnt = printResult(price_results, writer);
		} catch (SQLException e) {
			logger.error("SELECT statement failed", e);
		} finally {
			try {
				if (null != price_results) {
					price_results.close();
				}
				
				if (null != query_prices) {
					query_prices.close();
				}
			} catch (SQLException e) {
				logger.warn("Closing result set or closing prepared statement failed, giving up", e);
			}
		}
		/* END */
		return cnt;
	}

	/**
	 * Ausgabe des Durchschnittspreises, MIN, MAX aller CDs eines Kuenstlers bzgl.
	 * LowUsedPrice und LowNewPrice mit Hilfe von printResult.
	 * @param artist Name des Kuenstlers
	 * @param writer PrinterWriter, auf den das Ergebnis geschrieben werden soll
	 * @return int Anzahl der selektierten Tupel des Ergebnisses
	 * 
	 * 3 Punkte
	 */
	public int showGroupPricesArtist(String artist, PrintWriter writer) {
		int cnt = 0;
		/* BEGIN */
		PreparedStatement query_artist_prices = null;
		ResultSet artist_price_results = null;
		try {
			query_artist_prices = co.prepareStatement(
					"SELECT " +
					"DECIMAL(AVG(LOWNEWPRICE), 6, 2) AS AVG_NEW, " +
					"DECIMAL(AVG(LOWUSEDPRICE), 6, 2) AS AVG_USED, " +
					"MIN(LOWNEWPRICE) AS MIN_NEW, " +
					"MIN(LOWUSEDPRICE) AS MIN_USED, " +
					"MAX(LOWNEWPRICE) AS MAX_NEW, " +
					"MAX(LOWUSEDPRICE) AS MAX_USED " +
					"FROM CD " +
					"WHERE ARTIST = ?");
			query_artist_prices.setString(1, artist);

			artist_price_results = query_artist_prices.executeQuery();
			cnt = printResult(artist_price_results, writer);
		} catch (SQLException e) {
			logger.error("SELECT statement failed", e);
		} finally {
			try {
				if (null != artist_price_results) {
					artist_price_results.close();
				}
				
				if (null != query_artist_prices) {
					query_artist_prices.close();
				}
			} catch (SQLException e) {
				logger.warn("Closing result set or closing prepared statement failed, giving up", e);
			}
		}
		/* END */
		return cnt;
	}

	/**
	 * Aendert den LowNewPrice einer CD
	 * @param asin String
	 * @param price float
	 * 
	 * 2 Punkte
	 */
	public void updateNewPrice(String asin, float price)	{
		/* BEGIN */
		PreparedStatement price_update = null;
		try {
			price_update = co.prepareStatement(
					"UPDATE CD SET LOWNEWPRICE = ? WHERE ASIN = ?");
			price_update.setFloat(1, price);
			price_update.setString(2, asin);

			int rowcnt = price_update.executeUpdate();
			logger.debug(String.format("Updated rows: %d", rowcnt));
		} catch (SQLException e) {
			logger.error("UPDATE statement failed", e);
		} finally {
			try {
				if (null != price_update) {
					price_update.close();
				}
			} catch (SQLException e) {
				logger.warn("Could not close update statement, giving up", e);
			}
		}
		/* END */
	}

	/**
	 * Loescht eine CD und ihre Tracks aus der DB
	 * @param asin String
	 * 
	 * 3 Punkte
	 */
	public void deleteSingleCD(String asin)	{
		/* BEGIN */
		PreparedStatement delete_cd = null;
		try {
			delete_cd = co.prepareStatement(
					"DELETE FROM CD WHERE ASIN = ?");
			delete_cd.setString(1, asin);
			
			int rowcnt = delete_cd.executeUpdate();
			logger.debug(String.format("Deleted rows: %d", rowcnt));
		} catch (SQLException e) {
			logger.error("DELETE statement failed", e);
		} finally {
			try {
				if (null != delete_cd) {
					delete_cd.close();
				}
			} catch (SQLException e) {
				logger.warn("Could not close delete statement, giving up", e);
			}
		}
		/* END */
	}
}	
