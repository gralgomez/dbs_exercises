-- =================================================================
-- | WICHTIG:                                                      |
-- |   Dieses Skript muss von einer Shell auf den Vogelrechnern der| 
-- |   Studentenpools aus wie folgt aufrufbar sein:                |
-- |         db2 -tvf createMusicRelations.sql                     |
-- |    Der Dateiname ist nicht zu veraendern                      |
-- =================================================================

CONNECT TO DBPrak;

-- Aufblatt 5 ------------------------------------------------------

-- Aufgabe 1.1 -----------------------------------------------------
-- Erzeugen der Tabellen zum Speichern der Musik Daten -------------
--------------------------------------------------------------------
CREATE TABLE CD (
	ASIN CHAR(10) NOT NULL,
	Artist VARCHAR(100) NOT NULL,
	Label VARCHAR(100) NOT NULL,
	Title VARCHAR(100) NOT NULL,
	LowUsedPrice DECIMAL(6,2) NOT NULL,
	LowNewPrice DECIMAL(6,2) NOT NULL);

CREATE TABLE Disc (
	CD_ASIN CHAR(10) NOT NULL,
	Number INTEGER NOT NULL);

CREATE TABLE Track (
	CD_ASIN CHAR(10) NOT NULL,
	Disc_Number INTEGER NOT NULL,
	Title VARCHAR(100) NOT NULL,
	Number INTEGER NOT NULL);

-- Aufgabe 1.2 -----------------------------------------------------
-- Erstellen von Primaer-/Fremdschluesseln, Contraints, Triggern ---
--------------------------------------------------------------------
-- Primarykeys
ALTER TABLE CD
	ADD PRIMARY KEY (ASIN);

ALTER TABLE Disc
	ADD PRIMARY KEY (CD_ASIN, Number);
	
ALTER TABLE Track
	ADD PRIMARY KEY (CD_ASIN, Disc_Number, Title, Number);

-- Foreignkeys
ALTER TABLE Disc
	ADD CONSTRAINT fkey_cd
	FOREIGN KEY (CD_ASIN) REFERENCES CD(ASIN)
	ON DELETE CASCADE;

ALTER TABLE Track
	ADD CONSTRAINT fkey_disc
	FOREIGN KEY (CD_ASIN, Disc_Number) REFERENCES Disc(CD_ASIN, Number)
	ON DELETE CASCADE;
	
-- Constraints
ALTER TABLE CD
	ADD CONSTRAINT check_prices
	CHECK (LowUsedPrice >= DECIMAL(0, 6, 2) AND LowNewPrice >= DECIMAL(0, 6, 2));

ALTER TABLE Disc
	ADD CONSTRAINT check_disc_number
	CHECK (Number > INTEGER(0));

ALTER TABLE Track
	ADD CONSTRAINT check_track_number
	CHECK (Number > INTEGER(0));

-- Trigger
CREATE TRIGGER trg_update_prices
	AFTER UPDATE ON CD
	REFERENCING OLD AS old NEW AS new
	FOR EACH ROW
	MODE DB2SQL
	WHEN (old.LowNewPrice <> new.LowNewPrice)
	UPDATE CD
		SET LowUsedPrice = LowUsedPrice * new.LowNewPrice / old.LowNewPrice
		WHERE ASIN = new.ASIN;
	
-- evtl. Views
CREATE VIEW CD_with_disccnt AS
	SELECT c.ASIN, c.Artist, c.Label, c.Title, c.LowUsedPrice, c.LowNewPrice, COUNT(d.Number) AS DiscCnt
	FROM CD AS c, Disc AS d
	WHERE c.ASIN = d.CD_ASIN
	GROUP BY c.ASIN, c.Artist, c.Label, c.Title, c.LowUsedPrice, c.LowNewPrice;
	
CONNECT RESET;
