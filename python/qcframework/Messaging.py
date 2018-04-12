"""This module defines the Messaging Framework.
"""
import datetime
import sys
import traceback

import qcframework.Search as Search
from despydmdb import desdmdbi


class Messaging(object):
    """ Class to handle writing logs and scanning the input for messages that need to be inserted
        into the PFW_TASK_MESSAGE table. Patterns which indicate messages which need saving are provided
        in the PFW_MESSAGE_PATTERN table and patterns which indicated messages to be ignored are
        provided in the PFW_MESSAGE_IGNORE table.

        Keywords
        --------
        name : str
            Name of the log file to write to.
        execname : str
            String containing the name of the exec being monitored for output.
        pfwattid : int
            The current pfw_attempt_id.
        taskid : int, optional
            The task_id of the current task. Default: None
        dbh : DB handle, optional
            Handle to the database. If None then create one, default: None.
        mode : char, optional
            Character indicating the mode for opening the log file. Valid
            values are 'w' (write) and 'a' (append). Default: 'w'.
        buffering : int, optional
            The number of bytes to buffer before writing to the output log
            file. A value of 0 indicates no buffer (all output written
            immediately). Default: -1 (Python default buffering policy)
        usedb : boolean, optional
            If True then write matches to the database. Default: True
        qcf_patterns: dict, optional
            Dictionary containing patterns to match instead of those
            obtained from MEsSAGE_PATTERN. If None then use the values
            stored in the MESSAGE_PATTERN table. If a dict is given then the
            entries are assumed to be in descending order of priority (i.e.
            if one is found in a string then no others lower in the order
            are searched for). Keys are ignored, with the exception of
            'exclude' which is used to give the entries for the ignore list.
            Each pattern item must have an entry 'pattern' containing the
            individual pattern. Default: None
    """

    def __init__(self, name, execname, pfwattid, taskid=None, dbh=None, mode='w', buffering=-1,
                 usedb=True, qcf_patterns=None):
        if mode == 'r':
            raise Exception("Invalid mode for log file opening, valid values are 'w' or 'a'.")
        # set some initial values
        self._patterns = []
        self.ignore = []
        self._filter = []
        self._lineno = 0
        self.mlineno = 0
        self.usedb = usedb
        # open the log file if a name is given
        if name is not None:
            self.fname = name
            self._file = open(name, mode=mode, buffering=buffering)
        else:
            self.fname = ''
            self._file = None
        # set up the pattern dictionary if one is given on the command line
        override = False
        if qcf_patterns is not None:
            temppat = {}
            priority = 0
            if 'override' in list(qcf_patterns.keys()):
                if qcf_patterns['override'].upper() == 'TRUE':
                    override = True
            # loop over all patterns
            if 'patterns' in list(qcf_patterns.keys()):
                for n, pat in qcf_patterns['patterns'].items():
                    # if it lists the exclude items
                    # set up a full dict entry
                    priority += 1
                    # set default values
                    p = {'id': 9999,   # all user generated patterns must have an id of 9999
                         'used': 'y',
                         'lvl': 1,
                         'number_of_lines': 1,
                         'only_matched': 'N',
                         'priority': priority,
                         'execname': 'global'}
                    # get the pattern
                    if 'pattern' in list(pat.keys()):
                        p['pattern'] = pat['pattern']
                    else:
                        continue
                    # look for any other items and update as needed
                    if 'lvl' in list(pat.keys()):
                        p['lvl'] = int(pat['lvl'])
                    if 'priority' in list(pat.keys()):
                        p['priority'] = int(pat['priority'])
                    if 'execname' in list(pat.keys()):
                        p['execname'] = pat['execname']
                    if 'number_of_lines' in list(pat.keys()):
                        p['number_of_lines'] = int(pat['number_of_lines'])
                    if 'only_matched' in list(pat.keys()):
                        p['only_matched'] = pat['only_matched']
                    temppat[p['priority']] = p
            # now put them in order
            keys = list(temppat.keys())
            keys.sort()
            for k in keys:
                self._patterns.append(temppat[k])

            if 'excludes' in list(qcf_patterns.keys()):
                execs = execname.split(',') + ['global']
                for n, pat in qcf_patterns['excludes'].items():
                    if 'exec' in list(pat.keys()):
                        if not pat['exec'] in execs:
                            continue
                    self.ignore.append(pat['pattern'])
            if 'filter' in list(qcf_patterns.keys()):
                execs = execname.split(',') + ['global']
                for n, pat in qcf_patterns['excludes'].items():
                    if 'exec' in list(pat.keys()):
                        if not pat['exec'] in execs:
                            continue
                    patrn = {}
                    patrn['replace_pattern'] = pat['replace_pattern']
                    if 'with_pattern' in list(pat.keys()):
                        patrn['with_pattern'] = pat['with_pattern']
                    self._filter.append(patrn)

        # connect to the DB if needed
        if self.usedb:
            if dbh is None: # or not PING
                self.reconnect()
            else:
                self.dbh = dbh
                self.cursor = dbh.cursor()
        else:
            self.dbh = None
            self.cursor = None
        self._pfwattid = int(pfwattid)
        self._taskid = int(taskid)
        # get the patterns from the database if needed
        if usedb:
            if not override:
                self.cursor.execute("select id, pattern, lvl, only_matched, number_of_lines from ops_message_pattern where execname in ('global','%s') and used='y' order by priority" % (
                    execname.replace(',', "','")))
                desc = [d[0].lower() for d in self.cursor.description]
                for line in self.cursor:
                    self._patterns.append(dict(list(zip(desc, line))))

                self.cursor.execute("select pattern from ops_message_ignore where execname in ('global','%s') and  used='y'" % (
                    execname.replace(',', "','")))
                for line in self.cursor:
                    self.ignore.append(line[0])
            self.cursor.execute("select replace_pattern, with_pattern from ops_message_filter where execname in ('global','%s') and used='y'" % (
                execname.replace(',', "','")))
            desc = [d[0].lower() for d in self.cursor.description]
            for line in self.cursor:
                self._filter.append(dict(list(zip(desc, line))))
                if self._filter[-1]['with_pattern'] is None:
                    self._filter[-1]['with_pattern'] = ''
        pats = []
        self._traceback = -1
        # get the pattern id for the 'traceback' entry
        for pat in self._patterns:
            if 'Traceback' in pat['pattern']:
                self._traceback = pat['id']
            pats.append(pat['pattern'])
        # set up the search class
        self.search = Search.Search(pats, self.ignore)
        self._getmore = 0
        self._intraceback = False
        self._message = ""
        self._indx = None
        self._found = False

    def reconnect(self):
        """Method to reconnect to the database.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.dbh = desdmdbi.DesDmDbi()
        self.cursor = self.dbh.cursor()

    def setname(self, name):
        """Method to set the output file name.

        This will not create the file, but is used to insert messages directly.

        Parameters
        ----------
        name : str
            The name of the log file.

        Returns
        -------
        None
        """
        self.fname = name

    def write(self, text, tid=None):
        """ Method to scan the input for any patterns, add the line to the DB if a match is found,
            and write to the log file. Note that all input lines are written to the log file
            regardless of whether a pattern is found or not.

        Parameters
        ----------
        text : str
            The text to handle, multiline text is supported.
        tid : int, optional
            Task_id of the current task, can be used to override a prent
            task id. Default: None.
        """
        # filter out any unneeded text
        text = text.rstrip()
        if len(self._filter) > 0:
            for fltr in self._filter:
                text = text.replace(fltr['pattern_replace'], fltr['with_pattern'])
        # write out to the log
        if self._file is not None:
            self._file.write(text + "\n")
        # if not using the DB then exit
        if not self.usedb:
            return
        # split the text up into individual lines
        text_list = text.split("\n")
        # loop over each line
        for no, line in enumerate(text_list):
            # keep track of the log line number, this is not done for runjob.out as the parallel
            # threads cannot keep track of each others line numbers
            self._lineno += 1
            # if this is a multi line pattern
            if self._getmore > 0:
                self._message += "\n" + line
                # special handling for tracebacks
                if self._intraceback and 'File' in line:
                    self._getmore += 1
                else:
                    self._getmore -= 1
            else:
                # search for any pattern matches
                (self._indx, match) = self.search.search(line)
                # if there is a match
                if self._indx is not None:
                    # if the pattern is to only keep the text that matches
                    if self._patterns[self._indx]['only_matched'].upper() != 'N' and \
                       self._patterns[self._indx]['number_of_lines'] > 0:
                        self._message = match
                    else:
                        if self._patterns[self._indx]['id'] == self._traceback:
                            self._intraceback = True
                        self._message = line
                        # see if this is a multi line match
                        self._getmore = self._patterns[self._indx]['number_of_lines'] - 1
                        if line.endswith(':'):
                            self._getmore += 1
                    if 'runjob.out' in self.fname:
                        self.mlineno = 0
                    else:
                        # get the current line number
                        self.mlineno = no + self._lineno
                    self._found = True
                else:
                    self._getmore = -1

            # if there are no more lines to prcess for this particular match
            if self._getmore <= 0 and self._found:
                self._found = False
                self._intraceback = False
                # trim the length back to the size of the db column
                if len(self._message) > 4000:
                    self._message = self._message[:3998] + '?'
                # replace any single quotes with double so that they insert into the DB properly
                self._message = self._message.replace("'", '\'\'')

                if tid is None:
                    tid = self._taskid
                # make no more than two attempts at inserting the data into the DB
                for i in range(2):
                    try:
                        bind_vals = {'tid': tid,
                                     'pfwattid': self._pfwattid,
                                     'msg_time': self.search.findtime(text),
                                     'lvl': self._patterns[self._indx]['lvl'],
                                     'pat_id': self._patterns[self._indx]['id'],
                                     'message': self._message,
                                     'logfile': self.fname,
                                     'lineno': self.mlineno}
                        sql = "insert into task_message (task_id, pfw_attempt_id, message_time, message_lvl, ops_message_pattern_id, message, log_file, log_line) values (:tid, :pfwattid, TO_TIMESTAMP(:msg_time, 'YYYY-MM-DD HH24:MI:SS.FF'), :lvl, :pat_id, :message, :logfile, :lineno)"
                        self.cursor.execute(sql, **bind_vals)
                        #% (tid, self._pfwattid, self.search.findtime(text), self._patterns[self._indx]['lvl'], self._patterns[self._indx]['id'], self._message, self.fname, self.mlineno))
                        # commit the change in the case that the process dies, any error info may be saved first
                        self.cursor.execute("commit")
                        break
                    except:
                        # try reconnecting
                        self.reconnect()
                        # if two attempts have been made and failed then write a message to the log file
                        if i == 1:
                            if self._file is not None:
                                self._lineno += 1
                                self._file.write("QCF could not write the following to database:\n\t")
                                self._file.write(self._message)
                                (extype, exvalue, trback) = sys.exc_info()
                                traceback.print_exception(extype, exvalue, trback, file=self._file)

                # reset the message
                self._message = ""


def pfw_message(dbh, pfwattid, taskid, text, level, log_file='runjob.out', line_no=0):
    """Provide direct access to PFW_TASK_MESSAGE.

    Method to provide direct access to the PFW_TASK_MESSAGE for pfwrunjob.py
    for custom error messages.

    Parameters
    ----------
    pfwattid : int
        The current pfw_attempt_id
    taskid : int
        The currrent task_id
    text : str
        The text to insert into the DB
    level : int
        The level of the message (1=error, 2=warning, 3=info)
    """
    cursor = dbh.cursor()
    text2 = text.replace("'", '"')
    sql = "insert into task_message (task_id, pfw_attempt_id, message_time, message_lvl, ops_message_pattern_id, message, log_file, log_line) values (%i, %i, TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF'), %i, 0, '%s', '%s', %i)" \
        % (int(taskid), int(pfwattid), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), level, text2, logfile, line_no)
    cursor.execute(sql)
    cursor.execute("commit")
