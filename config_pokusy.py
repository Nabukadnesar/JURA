from threading import Thread, Timer
from datetime import datetime
from time import time, sleep
from itertools import cycle
import sqlite3 as lite
import configparser
import logging
import serial
import sys
import os


def logger_config():
    logger = logging.getLogger('DB')
    logger.setLevel(logging.DEBUG)
    # Nastaveni fileHandleru
    fh = logging.FileHandler(filename="log.txt", mode="w")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s'))
    logger.addHandler(fh)
    # Nastaveni consoleHandleru
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(logging.Formatter('%(levelname)-8s | %(message)s'))
    logger.addHandler(sh)
    logger.info("Start logging")
    return logger


class ConfigDecode(object):

    def __init__(self):
        self.cwd = self.get_cwd()
        self.config_name = "config.ini"
        self.config = configparser.ConfigParser()
        self.config_read()
        self.get_dbSection()
        self.get_serSection()
        self.get_mainSection()

    def get_cwd(self):
        cwd_path = os.path.dirname(os.path.realpath(__file__))
        logger.info("Current working directory: {}".format(cwd_path))
        return cwd_path

    def config_read(self):
        config_path = os.path.join(self.cwd, self.config_name)
        if os.path.isfile(config_path):
            logger.info("Config file path: {}".format(config_path))
        else:
            # Vytvoreni defaultniho config souboru
            defConfig = configparser.ConfigParser(allow_no_value=True)
            section = "DB"
            defConfig.add_section(section)
            defConfig.set(section, "# nazev vytvorene databaze, pokud neexistuje, bude vytvorena")
            defConfig.set(section, "db_name", "default.db")
            defConfig.set(section, "\n# nazev tabulky v databazi")
            defConfig.set(section, "table_name", "test")
            defConfig.set(section, "\n# nazvy merenych kanalu v DB, nazvy oddelene carkou (napr: CH1,CH2,CH3,..)")
            defConfig.set(section, "channels", "CH1, CH2, CH3")

            section = "SERIAL"
            defConfig.add_section(section)
            defConfig.set(section, "port", "COM1")
            defConfig.set(section, "baudrate", "9600")
            defConfig.set(section, "timeout", "1")

            section = "MAIN"
            defConfig.add_section(section)
            defConfig.set(section, "# interval mezi dotazy o nova data v nekonecne smycce (hodnota v sekundach)")
            defConfig.set(section, "measure_interval", "120")
            defConfig.set(section, "\n# uroven logovani pro LOG soubor (log.txt), vyber z debug, info, warning, error")
            defConfig.set(section, "loglevel", "debug")

            with open(config_path, 'w') as fp:
                defConfig.write(fp)
            logger.info("New config file created: {}".format(config_path))
        self.config.read(config_path)

    def get_dbSection(self):
        section = "DB"
        try:
            self.db_name = self.config[section]["db_name"]
            self.table_name = self.config[section]["table_name"]
            names = self.config[section]["channels"]
            self.chann_names = [x.strip() for x in names.split(',')]
        except KeyError as err:
            sys.exit(logger.error('Config file parse: KeyError: {} in DB section'.format(err)))

    def get_serSection(self):
        section = "SERIAL"
        try:
            self.ser_port = self.config[section]["port"]
            self.ser_baudrate = int(self.config[section]["baudrate"])
            self.ser_timeout = int(self.config[section]["timeout"])
        except KeyError as err:
            sys.exit(logger.error('Config file parse: KeyError: {} in SERIAL section'.format(err)))

    def get_mainSection(self):
        section = "MAIN"
        try:
            self.main_measInterval = self.config[section]["measure_interval"]
            self.main_logLevel = self.config[section]["loglevel"]
        except KeyError as err:
            sys.exit(logger.error('Config file parse: KeyError: {} in MAIN section'.format(err)))


class DB_thread(Thread):

    def __init__(self):
        super().__init__()
        # Daemon Thread
        self.setDaemon = True
        # DB SECTION from config file
        self.db_name = conf.db_name
        self.table_name = conf.table_name
        self.channels = conf.chann_names
        # Path SECTION
        self.cwd = conf.cwd
        self.db_path = os.path.join(self.cwd, self.db_name)
        # DATABASE HANDLE
        self.db_initialCheck()

    def db_initialCheck(self):
        logger.debug("########## START database initial check ##########")
        logger.info("DB: Database settings from config file: {dn},{tn},{chn}"
                    .format(dn=self.db_name, tn=self.table_name, chn=self.channels))
        self.con = self.get_connector()
        self.tabNames = self.get_tableNames()
        self.check_table()
        db_initLen = len(self.exist_colNames)-2     # minus Id a Timestamp
        ser_initLen = len(SER.init_data)-1          # minus ID
        logger.debug("DB: Active columns: {dc}, SER: Active data length: {sc}".format(dc=db_initLen, sc=ser_initLen))
        if (db_initLen) == (ser_initLen):
            logger.info("SER and DB initial consistency OK")
        else:
            sys.exit(logger.error("SER and DB initial consistency FAIL (db_initialCheck)"))

    def get_connector(self):
        if os.path.isfile(self.db_path):
            logger.debug("DB: Database already exists: {}".format(self.db_path))
        else:
            logger.debug("DB: New database created: {}".format(self.db_path))
        con = lite.connect(os.path.join(self.cwd, self.db_name))
        logger.debug("DB: Create connector to: {}".format(self.db_name))
        return con

    def get_tableNames(self):
        with self.con:
            try:
                cur = self.con.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tab_names = [t[0] for t in cur.fetchall()]
                logger.debug("DB: Available tables: {}".format(tab_names))
                return tab_names
            except lite.Error as err:
                sys.exit(logger.error("DB get list of table names: {}".format(err)))

    def get_tableHeaders(self):
        with self.con:
            try:
                cur = self.con.cursor()
                cur.execute("PRAGMA table_info({tn})".format(tn=self.table_name))
                col_names = [c[1] for c in cur.fetchall()]
                return col_names
            except lite.Error as err:
                sys.exit(logger.error("DB get list of table headers: {}".format(err)))

    def check_table(self):
        with self.con:
            try:
                cur = self.con.cursor()
                # Kontrola vytvorenych tabulek v DB
                if self.table_name in self.tabNames:
                    col_names = self.get_tableHeaders()
                    logger.debug("DB: Table '{tn}' already exists with column names {cn}"
                                 .format(tn=self.table_name, cn=col_names))
                    # TODO pridat moznost prepsani tabulky
                else:
                    # Vytvoreni tabulky podle konfigu
                    sens = ", ".join(["{} TEXT".format(name) for name in self.channels])
                    cur.execute("CREATE TABLE IF NOT EXISTS {tn}(Id INTEGER PRIMARY KEY, Timestamp TEXT, {chn})"
                                .format(tn=self.table_name, chn=sens))
                    logger.info("DB: Create new table '{tn}' in '{dn}' database"
                                .format(tn=self.table_name, dn=self.db_name))

                # Kontrola konzistence hlavicek v tabulce (exist vs config)
                config_colNames = ["Id", "Timestamp"] + self.channels
                self.exist_colNames = self.get_tableHeaders()
                if config_colNames != self.exist_colNames:
                    sys.exit(logger.error("DB column names non-consistency in table '{tn}', exist: {cn1}, config: {cn2}"
                                          .format(tn=self.table_name, cn1=self.exist_colNames, cn2=config_colNames)))

                # Logging aktivni tabulky a nazvu jejich sloupcu

                logger.info("DB: Active table: '{tn}' with column names: {cn} and length {ln}"
                            .format(tn=self.table_name, cn=self.exist_colNames, ln=self.get_tableLen()))

            except lite.Error as err:
                sys.exit(logger.error("DB check/create table: {}".format(err)))

    def db_insert(self, data, dTime):
        with self.con:
            try:
                cur = self.con.cursor()
                vals = data
                vals.insert(0, str(datetime.now())[:-3])
                sens_names = ",".join([name for name in self.channels])
                vals_names = ",".join("?" * (len(self.channels) + 1))
                cur.execute("INSERT INTO {tn} (Timestamp,{sn}) VALUES ({vn})"
                            .format(tn=self.table_name, sn=sens_names, vn=vals_names), vals)
                # Specialni PRINT pouze do konzole + prepis jednoho radku
                logger.info("DB: new record in {tn}: {d}, Timeout: {t:.3f}"
                             .format(tn=self.table_name, d=vals, t=dTime))
                print("\r Last DB log: {d}, Timeout: {t:.3f}"
                      .format(d=vals, t=dTime), end="\r", flush=True)
            except lite.Error as err:
                errMsg = "ERROR during DB insert values: {e}".format(e=err)
                logger.error(errMsg)
                print("\r "+errMsg, end="\r", flush=True)

    def get_tableLen(self):
        with self.con:
            try:
                cur = self.con.cursor()
                cur.execute("SELECT COUNT(ROWID) FROM {tn}".format(tn=self.table_name))
                nRows = cur.fetchone()[0]
                return nRows
            except lite.Error as err:
                sys.exit(logger.error("DB get_tableLen: {}".format(err)))

    def db_close(self):
        if self.con:
            self.con.close()
            logger.info("DB: connection close regularly")

    def run(self):
        measInterval = int(conf.main_measInterval)
        while True:
            try:
                t1 = time()
                data, dTime = SER.serial_measure()
                self.db_insert(data, dTime)
                t2 = time()
                sleep(measInterval-(t2-t1))
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt detect: try to regular termination...")
                break
            except Exception as err:
                logger.error("Error detected in main thread (run): {}".format(err))
                break
        # Regulerni ukonceni DB
        self.db_close()


class SerialHandle(object):

    def __init__(self):
        self.EOL = "\r\n"
        # self.tastMsg = "data(ID,T0,T1,T2,T3,T4,TL1,TL2,P1,P2)"
        self.ID = ID = cycle(range(10))
        # SERIAL SECTION from config file
        self.ser_Port = conf.ser_port
        self.ser_Rate = conf.ser_baudrate
        self.ser_Timeout = conf.ser_timeout
        # INIT CHECK
        self.serial_initialCheck()

    def serial_getInfo(self):
        try:
            with serial.Serial(self.ser_Port, self.ser_Rate, timeout=self.ser_Timeout) as ser:
                ser.reset_input_buffer()
                ser.write("info{0}".format(self.EOL).encode())
                line = ser.readline().decode().split(self.EOL)[0]
                logger.debug("SER: serial_getInfo: {}".format(line))
            return line
        except Exception as err:
            logger.error("SER: Serial connection FAIL (serial_getInfo): {}".format(err))

    def serial_getData(self):
        try:
            lastID = next(self.ID)
            with serial.Serial(self.ser_Port, self.ser_Rate, timeout=self.ser_Timeout) as ser:
                ser.reset_input_buffer()
                ser.write("getdata({0}){1}".format(lastID, self.EOL).encode())
                # TODO timeout poresit
                line = ser.readline().decode().split(self.EOL)[0]
                # TODO zkontrolovat buffer
                logger.debug("SER: serial_getData: {}".format(line))
            return line, lastID
        except Exception as err:
            logger.error("SER: Serial connection FAIL (serial_getData): {}".format(err))

    def serial_measure(self):
        start_time = time()
        data_raw, lastID = self.serial_getData()
        data = self.serial_decodeData(data_raw)
        if data:
            decodeID = int(data[0])
        else:
            decodeID = -1
        delta_time = time()-start_time
        logger.debug("SER: ID_in: {IDin}, ID_out: {IDout}, Time: {tm:.3f}s"
                     .format(IDin=lastID, IDout=decodeID, tm=delta_time))
        if (lastID == decodeID):
            data = data[1:]     # zbavit se ID
        else:
            data = []
        return data, delta_time

    def serial_initialCheck(self):
        logger.debug("########## START serial initial check ##########")
        logger.info("SER: Serial settings from config file: {port},{rate},{to}"
                    .format(port=self.ser_Port, rate=self.ser_Rate, to=self.ser_Timeout))
        self.init_infoMsg = self.serial_getInfo()
        data_raw, lastID = self.serial_getData()
        self.init_data = self.serial_decodeData(data_raw)
        if (self.init_infoMsg == "ProfSenzor Ska2018") and (self.init_data):
            logger.info("SER: Serial connection OK")
        else:
            sys.exit(logger.error("SER: Serial connection FAIL (serial_initialCheck)"))

    def serial_decodeData(self, line):
        if (line == "ERR") or (line == None):
            data = []
            # TODO REAKCE NA ERR nebo NONE zpravu
        else:
            data = line[line.find("(") + 1:line.find(")")].split(",")
            logger.debug("SER: serial_decodeData: {}".format(data))
        return data

    def testID(self):
        print(next(self.ID))


# Hlavni telo skriptu
if __name__ == '__main__':
    logger = logger_config()
    conf = ConfigDecode()

    logLevel = logging.getLevelName(conf.main_logLevel.upper())
    logger.setLevel(logLevel)

    SER = SerialHandle()
    DB = DB_thread()

    save_handlers = logger.handlers
    logger.handlers = [logger.handlers[0]]
    print("\n", end="", flush=True)
    DB.run()

    # ukonceni
    logger.handlers = save_handlers
    print("\n", flush=True)
    logger.info("Stop logging")
