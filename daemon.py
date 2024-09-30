import mysql.connector
import time
import logging
import base64
import subprocess
import json

from timeit import default_timer as timer
from types import FunctionType as function

import processPDF as pdf
import processXML as xml
from config import DRUPAL_PATH, LOG_PATH, DB_CONFIG

CYCLE_TIME = 120
PARSE_LIMIT = 10
CREATE_LIMIT = 1200

# supported file types:
# dictionary with following format:
#  key:  file type extension
#  value: function to process that type
FILE_TYPES: dict[str, function] = {
    "pdf": pdf.process,
    "xml": xml.process
}

TRANSACTION_LEVEL_QUERY = '''
    SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED
'''
GET_PATHS_QUERY = '''SELECT ID, pdfPath, processPath, entityId
                     FROM FilePaths
                     WHERE failed = 0 LIMIT {}'''
UPDATE_PATH_QUERY = '''UPDATE FilePaths
                       SET failed = 1
                       WHERE ID = {}'''
DROP_PATH_QUERY = '''DELETE FROM FilePaths WHERE ID = {}'''
MAKE_DOC_QUERY = '''INSERT INTO DocObjs (title, metadata, entityId, numLinks)
                    VALUES ("{}", "{}", {}, {})'''
MAKE_LINK_QUERY = '''INSERT INTO LinkObjs (fromTitle, toTitle, pages)
                     VALUES ("{}", "{}", "{}")'''
CHECK_REMAINING_QUERY = '''SELECT SUM(rowCount) FROM (
                           SELECT COUNT(*) AS rowCount
                               FROM FilePaths WHERE failed = 0
                           UNION ALL
                           SELECT COUNT(*) AS rowCount
                               FROM DocObjs WHERE failed = 0
                           UNION ALL
                           SELECT COUNT(*) AS rowCount
                               FROM LinkObjs WHERE failed = 0)
                           AS tmp'''


def timeNow():
    return time.ctime(time.time())


logger = logging.getLogger("LRVSP_Python")
logging.basicConfig(filename=f"{LOG_PATH}",
                    encoding="utf8",
                    level=logging.DEBUG)

logger.info(f"\t{timeNow()}\t| Start daemon")

try:
    while True:
        startTime = timer()
        msg = "\t{}\t| Start processing"
        logger.info(msg.format(timeNow()))
        # open database connection
        cnx = mysql.connector.connect(**DB_CONFIG)
        cursor = cnx.cursor()
        # set transaction level
        cursor.execute(TRANSACTION_LEVEL_QUERY)

        # get filepaths to process
        cursor.execute(GET_PATHS_QUERY.format(PARSE_LIMIT))
        # extract all results in cursor iterator, free it for use elsewhere
        results = [res for res in cursor]
        # commit a select statement??????
        # IDK why this needs to be here. But it does.
        cnx.commit()

        for res in results:
            # start transaction
            cnx.start_transaction(isolation_level="READ COMMITTED")

            # get results
            pathId: int = res[0]
            pdfPath: str = res[1]
            processPath: str = res[2]
            entId: int = res[3]

            # use process path for processing if possible,
            # otherwise use the pdf
            if processPath == "":
                file = pdfPath
            else:
                file = processPath
            
            # get file type
            fType = file.split('.')[-1].lower()
            fName = file.split('/')[-1].removesuffix(fType)
                
            if fType in FILE_TYPES:
                msg = "\t{}\t| Processing new {}: {}"
                logger.info(msg.format(timeNow(), fType, fName))
                try:
                    result = FILE_TYPES[fType](file)
                except Exception as e:
                    msg = "\t{}\t| File processing failed, message: {}"
                    logger.info(msg.format(timeNow(), e))
                    # update entry to let drupal know it failed
                    cursor.execute(UPDATE_PATH_QUERY.format(pathId))
                    cnx.commit()
                    continue
            else:
                msg = "\t{}\t| Unsupported File type: {}"
                logger.error(msg.format(timeNow(), fType))
                # update entry to let drupal know it failed
                cursor.execute(UPDATE_PATH_QUERY.format(pathId))
                cnx.commit()
                continue

            # check that the processing returned the correct thing
            if not isinstance(result, dict):
                msg1 = "\t{}\t| File processing did not complete."
                msg = " Expected dict, got {}"
                logger.error((msg1 + msg2).format(timeNow(), type(result)))
                # update entry to let drupal know it failed
                cursor.execute(UPDATE_PATH_QUERY.format(pathId))
                cnx.commit()
                continue

            # read data from result:
            try:
                # name
                b64Name = base64.b64encode(
                    result["name"].encode()
                ).decode()
                # metadata
                metadata = base64.b64encode(
                    json.dumps(result["metadata"]).encode()
                ).decode()
                links = result["links"]
            except Exception:
                msg1 = "\t{}\t| File processing did not complete."
                msg2 = " returned dict does not contain required keys."
                logger.error((msg1 + msg2).format(timeNow))
                # update entry to let drupal know it failed
                cursor.execute(UPDATE_PATH_QUERY.format(pathId))
                cnx.commit()
                continue

            # remove path from database
            try:
                cursor.execute(DROP_PATH_QUERY.format(pathId))

                # push new DocObj to database
                cursor.execute(MAKE_DOC_QUERY.format(b64Name,
                                                     metadata,
                                                     entId,
                                                     len(links)))

                # push links to database
                for link in links:
                    b64Link = base64.b64encode(link.encode()).decode()
                    cursor.execute(MAKE_LINK_QUERY.format(b64Name,
                                                          b64Link,
                                                          "[]"))
            except mysql.connector.Error as e:
                msg = "\t{}\t| Error pushing to database: {}"
                logger.error(msg.format(timeNow(), e))
                # undo pushes
                cnx.rollback()
                # set failed
                cursor.execute(UPDATE_PATH_QUERY.format(pathId))
            except Exception as e:
                msg = "\t{}\t| Non msql error pushing to database: {}"
                logger.error(msg.format(timeNow(), e))
                # undo pushes
                cnx.rollback()
                # set path as failed
                cursor.execute(UPDATE_PATH_QUERY.format(pathId))
            finally:
                cnx.commit()

        # tell drupal to start processing
        result = subprocess.run([f"{DRUPAL_PATH}/vendor/bin/drush",
                                 "lrvsCheck-db",
                                 str(CREATE_LIMIT)])
        # if result.returncode == 0:
        #     res = result.stdout
        #     # decode into str if bytes returned
        #     if isinstance(res, bytes):
        #         res = res.decode()
        #     logger.info(f"\t{timeNow()}\t| {res}")
        # else:
        #     res = result.stderr
        #     # decode into str if bytes returned
        #     if isinstance(res, bytes):
        #         res = res.decode()
        #     msg = "\t{}\t| Drush failed with error code {} {}"
        #     logger.error(msg.format(timeNow(), result.returncode, res))
        # determine how long this took
        endTime = timer()
        timeTaken = endTime-startTime
        msg = "\t{}\t| End processing. Time taken {} seconds"
        logger.info(msg.format(timeNow(), timeTaken))

        # check if there's still stuff to process,
        # if there is immediately re-run
        cursor.execute(CHECK_REMAINING_QUERY)
        rowsLeft = next(cursor)
        cnx.close()
        if rowsLeft[0] == 0:
            time.sleep(CYCLE_TIME-min(CYCLE_TIME, timeTaken))

except KeyboardInterrupt:
    msg = "\t{}\t| Received keyboard interrupt, closing daemon"
    logger.info(msg.format(timeNow()))
