"""Define a database utility class.

It extends DM DB class with QCF specific functionality.
"""

__version__ = "$Rev: 38172 $"

import os
import sys
import traceback

import despydmdb.desdmdbi as desdmdbi
import despymisc.miscutils as miscutils


class QCFDB (desdmdbi.DesDmDbi):
    """Extend DM DB class with QCF specific functionality.
    """

    def __init__(self, desfile=None, section=None):
        try:
            desdmdbi.DesDmDbi.__init__(self, desfile, section)
        except Exception as err:
            miscutils.fwdie(
                "Error: problem connecting to database: %s\n\tCheck desservices file and environment variables" % err, 1)

    def get_qcf_messages_for_wrappers(self, wrapids):
        """Query and return rows from QC_PROCESSED_MESSAGE table.

        Those rows  are associated with the given wrapids. This assumes
        wrapids is a list of ids corresponding to the pfw_wrapper_id column.

        Parameters
        ----------
        wrapids : list
            List containing the wrapper ids.

        Returns
        -------
        Dictionary containing the messages (and associated data) from
        the requested ids.
        """
        # generate the sql
        sql = "select * from task_message where task_id=%s" % (self.get_positional_bind_string(1))
        #miscutils.fwdebug(0, 'QCFDB_DEBUG', "sql = %s" % sql)
        #miscutils.fwdebug(0, 'QCFDB_DEBUG', "wrapids = %s" % wrapids)
        # get a cursor and prepare the query
        curs = self.cursor()
        curs.prepare(sql)
        qcmsg = {}
        # execute the query for each given id and collect the results
        for id in wrapids:
            curs.execute(None, [id])
            desc = [d[0].lower() for d in curs.description]
            for line in curs:
                d = dict(list(zip(desc, line)))
                if d['task_id'] not in qcmsg:
                    qcmsg[d['task_id']] = []

                qcmsg[d['task_id']].append(d)

        return qcmsg
