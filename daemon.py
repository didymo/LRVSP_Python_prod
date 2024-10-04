import os
import sys

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

from constants import CYCLE_TIME, PARSE_LIMIT, CREATE_LIMIT


# supported file types:
# dictionary with following format:
#  key:  file type extension
#  value: function to process that type
FILE_TYPES: dict[str, function] = {
    "pdf": pdf.process,
    "xml": xml.process
}

from queries import (TRANSACTION_LEVEL_QUERY, GET_PATHS_QUERY,
                     UPDATE_PATH_QUERY, DROP_PATH_QUERY,
                     MAKE_DOC_QUERY, MAKE_LINK_QUERY,
                     CHECK_REMAINING_QUERY)


def timeNow():
    return time.ctime(time.time())




logger = logging.getLogger("LRVSP_Python")
logging.basicConfig(filename=f"{LOG_PATH}",
                    encoding="utf8",
                    level=logging.DEBUG)

logger.info(f"\t{timeNow()}\t| Start daemon")

def main():
    logger.info(f"\t{timeNow()}\t| Start daemon")
    try:
        while True:
            startTime = timer()
            logger.info(f"\\t{timeNow()}\\t| Start processing")
            # open database connection
            cnx = mysql.connector.connect(**DB_CONFIG)
            cursor = cnx.cursor()
            # set transaction level
            # cursor.execute(TRANSACTION_LEVEL_QUERY)

            # get filepaths to process
            cursor.execute(GET_PATHS_QUERY, (PARSE_LIMIT,))

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
                        cursor.execute(UPDATE_PATH_QUERY, (pathId,))
                        cnx.commit()
                        continue
                else:
                    msg = "\t{}\t| Unsupported File type: {}"
                    logger.error(msg.format(timeNow(), fType))
                    # update entry to let drupal know it failed
                    cursor.execute(UPDATE_PATH_QUERY, (pathId,))
                    cnx.commit()
                    continue

                # check that the processing returned the correct thing
                if not isinstance(result, dict):
                    msg1 = "\t{}\t| File processing did not complete."
                    msg = " Expected dict, got {}"
                    logger.error((msg1 + msg2).format(timeNow(), type(result)))
                    # update entry to let drupal know it failed
                    cursor.execute(UPDATE_PATH_QUERY, (pathId,))
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
                    cursor.execute(UPDATE_PATH_QUERY, (pathId,))
                    cnx.commit()
                    continue

                # remove path from database
                try:
                    cursor.execute(DROP_PATH_QUERY, (pathId,))

                    # push new DocObj to database
                    cursor.execute(MAKE_DOC_QUERY, (b64Name,
                                                    metadata,
                                                    entId,
                                                    len(links)))

                    # push links to database
                    for link in links:
                        b64Link = base64.b64encode(link.encode()).decode()
                        cursor.execute(MAKE_LINK_QUERY, (b64Name,
                                                         b64Link,
                                                         "[]"))
                except mysql.connector.Error as e:
                    msg = "\t{}\t| Error pushing to database: {}"
                    logger.error(msg.format(timeNow(), e))
                    # undo pushes
                    cnx.rollback()
                    # set failed
                    cursor.execute(UPDATE_PATH_QUERY, (pathId,))
                except Exception as e:
                    msg = "\t{}\t| Non msql error pushing to database: {}"
                    logger.error(msg.format(timeNow(), e))
                    # undo pushes
                    cnx.rollback()
                    # set path as failed
                    cursor.execute(UPDATE_PATH_QUERY, (pathId,))
                finally:
                    cnx.commit()

            # tell drupal to start processing
            # Ensure DRUPAL_PATH is absolute and correctly joined with the path to 'drush'
            drush_path = os.path.join(os.path.abspath(DRUPAL_PATH), 'vendor', 'bin', 'drush')

            try:
                result = subprocess.run([drush_path, "lrvsCheck-db", str(CREATE_LIMIT)],
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT,
                                        check=True)  # 'check=True' raises CalledProcessError for non-zero exit code

                res = result.stdout
                if isinstance(res, bytes):
                    res = res.decode()
                logger.info(f"\\t{timeNow()}\\t| Drush command succeeded with output length {len(res)}")

            except subprocess.CalledProcessError as e:
                logger.error(f"\\t{timeNow()}\\t| Drush failed with error code {result.returncode}")


            except Exception as e:
                logger.error(f"\\t{timeNow()}\\t| Unhandled exception in subprocess: {str(e)}")

            # determine how long this took
            endTime = timer()
            timeTaken = endTime - startTime
            logger.info(f"\\t{timeNow()}\\t| End processing. Time taken {timeTaken} seconds")

            # check if there's still stuff to process,
            # if there is immediately re-run
            cursor.execute(CHECK_REMAINING_QUERY)
            rowsLeft = next(cursor)
            cnx.close()
            if rowsLeft[0] == 0:
                time.sleep(CYCLE_TIME - min(CYCLE_TIME, timeTaken))

    except KeyboardInterrupt:
        logger.info(f"\t{timeNow()}\t| Received keyboard interrupt, closing daemon")
        return 0
    except Exception as e:
        logger.error(f"\t{timeNow()}\t| Unhandled exception: {str(e)}")
        return 1
    finally:
        if 'cnx' in locals() and cnx.is_connected():
            cnx.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
