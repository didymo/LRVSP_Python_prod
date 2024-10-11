# queries.py

TRANSACTION_LEVEL_QUERY = '''
    SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED
'''
GET_PATHS_QUERY = '''
    SELECT ID, pdfPath, processPath, entityId
    FROM FilePaths
    WHERE failed = 0
    LIMIT %s
'''
UPDATE_PATH_QUERY = '''
    UPDATE FilePaths
    SET failed = 1
    WHERE ID = %s
'''
DROP_PATH_QUERY = '''
    DELETE FROM FilePaths WHERE ID = %s
'''
MAKE_DOC_QUERY = '''
    INSERT INTO DocObjs (title, metadata, entityId, numLinks)
    VALUES (%s, %s, %s, %s)
'''
MAKE_LINK_QUERY = '''
    INSERT INTO LinkObjs (fromTitle, toTitle, pages)
    VALUES (%s, %s, %s)
'''
CHECK_REMAINING_QUERY = '''
    SELECT SUM(rowCount) FROM (
        SELECT COUNT(*) AS rowCount
            FROM FilePaths WHERE failed = 0
        UNION ALL
        SELECT COUNT(*) AS rowCount
            FROM DocObjs WHERE failed = 0
        UNION ALL
        SELECT COUNT(*) AS rowCount
            FROM LinkObjs WHERE failed = 0
    ) AS tmp
'''
