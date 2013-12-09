# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Define a database utility class extending coreutils.DesDbi
"""

__version__ = "$Rev$"

import os
import sys
import traceback

import coreutils
from coreutils.miscutils import *


class QCFDB (coreutils.DesDbi):
    """
        Extend coreutils.DesDbi to add database methods specific to QCF
    """

    def __init__ (self, *args, **kwargs):
        fwdebug(3, 'QCFDB_DEBUG', args)
        try:
            coreutils.DesDbi.__init__ (self, *args, **kwargs)
        except Exception as err:
            fwdie("Error: problem connecting to database: %s\n\tCheck desservices file and environment variables" % err, 1)
            
    def get_qcf_messages_for_wrappers(self, wrapids):
        """ query and return rows from QC_PROCESSED_MESSAGE table """
        # assumes wrapids is a list of ids corresponding to the pfw_wrapper_id column

        sql = "select * from qc_processed_message where pfw_wrapper_id=%s" % (self.get_positional_bind_string(1))
        fwdebug(0, 'QCFDB_DEBUG', "sql = %s" % sql)
        fwdebug(0, 'QCFDB_DEBUG', "wrapids = %s" % wrapids)
        curs = self.cursor()
        curs.prepare(sql)
        qcmsg = {}
        for id in wrapids:
            curs.execute(None, [id])
            desc = [d[0].lower() for d in curs.description]
            for line in curs:
                d = dict(zip(desc, line))
                d['message'] = d['message'].read()  # convert clob into string
                if d['pfw_wrapper_id'] not in qcmsg:
                    qcmsg[d['pfw_wrapper_id']] = []

                qcmsg[d['pfw_wrapper_id']].append(d)

        return qcmsg
