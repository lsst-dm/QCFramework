""" This module defines the Messageing Framework

"""
import datetime

import qcframework.Search as Search
from despydmdb import desdmdbi

class Messaging(file):
    """ Class to handle writing logs and scanning the input for messages that need to be inserted
        into the TASK_MESSAGE table. Patterns which indicate messages which need saving are provided
        in the MESSAGE_PATTERN table and patterns which indicated messages to be ignored are
        provided in the MESSAGE_IGNORE table.

        Keywords
        --------
        name : str
            Name of the log file to write to.

        execname: str
            String containing the name of the exec being monitored for output

        pfwattid : int
            Int containing the current pfw_attempt_id.

        taskid : int
            Int containing the task_id of the current task
            Default: None

        dbh : DB handle
            Handle to the database. If None then create one.
            Default: None

        mode : char
            Character indicating the mode for opening the log file. Valid values are 'w' (write) and
            'a' (append).
            Default: 'w'

        buffering : int
            The number of bytes to buffer before writing to the output log file. A value of 0
            indicates no buffer (all output written immediately).
            Default: 0

        filtr : boolean
            If True then filter non-ASCII values from the input strings before processing them.
            Default: False

        usedb : boolean
            If True then write matches to the database.
            Default: True

        qcf_patterns: dict
            Dictionary containing patterns to match instead of those obtained from MEsSAGE_PATTERN.
            If None then use the values stored in the MESSAGE_PATTERN table. If a dict is given
            then the entries are assumed to be in descending order of priority (i.e. if one is
            found in a string then no others lower in the order are searched for). Keys are ignored,
            with the exception of 'exclude' which is used to give the entries for the ignore list.
            Each pattern item must have an entry 'pattern' containing the individual pattern.
            Default: None

    """
    def __init__(self, name, execname, pfwattid, taskid=None, dbh=None, mode='w', buffering=0,
                 filtr=False, usedb=True, qcf_patterns=None):
        if mode == 'r':
            raise Exception("Invalid mode for log file opening, valid values are 'w' or 'a'.")
        # set some initial values
        self._patterns = []
        self.ignore = []
        self._lineno = 0
        # open the log file if a name is given
        if name is not None:
            self._file = True
            self.fname = name
            file.__init__(self, name=name, mode=mode, buffering=buffering)
        else:
            self.fname = ''
            self._file = False
        # set up the pattern dictionary if one is given on the command line
        if qcf_patterns is not None:
            temppat = {}
            priority = 0
            # loop over all patterns
            for n, pat in qcf_patterns.iteritems():
                # if it lists the exclude items
                if n == 'exclude':
                    self.ignore = qcf_patterns['exclude'].split(',')
                else:
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
                    if 'pattern' in pat.keys():
                        p['pattern'] = pat['pattern']
                    else:
                        continue
                    # look for any other items and update as needed
                    if 'lvl' in pat.keys():
                        p['lvl'] = int(pat['lvl'])
                    if 'priority' in pat.keys():
                        p['priority'] = int(pat['priority'])
                    if 'execname' in pat.keys():
                        p['execname'] = pat['execname']
                    if 'number_of_lines' in pat.keys():
                        p['number_of_lines'] = int(pat['number_of_lines'])
                    if 'only_matched' in pat.keys():
                        p['only_matched'] = pat['only_matched']
                    temppat[p['priority']] = p
            # now put them in order
            keys = temppat.keys()
            keys.sort()
            for k in keys:
                self._patterns.append(temppat[k])
        # connect to the DB if needed
        if dbh is None and usedb: # or not PING
            self.reconnect()
        else:
            self.dbh = dbh
            self.cursor = dbh.cursor()
        self._pfwattid = int(pfwattid)
        self._taskid = int(taskid)
        self._filter = filtr
        # get the patterns from the database if needed
        if usedb:
            if len(self._patterns) == 0:
                self.cursor.execute("select id, pattern, lvl, only_matched, number_of_lines from message_pattern where execname in ('global','%s') and used='y' order by priority" % (execname.replace(',', "','")))
                desc = [d[0].lower() for d in self.cursor.description]
                for line in self.cursor:
                    self._patterns.append(dict(zip(desc, line)))
            if len(self.ignore) == 0:
                self.cursor.execute("select pattern from message_ignore where used='y'")
                for line in self.cursor:
                    self.ignore.append(line[0])
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
        """ Method to reconnect to the database

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
        """ Method to set the output file name. This will not create the file, but is used to insert
            messages directly.

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
                The text to handle, multiline text is supported

            tid : int
                Task_id of the current task, can be used to override a prent task id.
                Default: None

        """
        # filter out any unneeded text
        text = text.rstrip()
        if self._filter:
            text = text.replace("[1A", "")
            text = text.replace(chr(27), "")
            text = text.replace("[1M", "")
            text = text.replace("[7m", "")
        # write out to the log
        if self._file:
            file.write(self, text + "\n")

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
                self._message = self._message.replace("'", '"')

                if tid is None:
                    tid = self._taskid
                # make no more than two attempts at inserting the data into the DB
                for i in range(2):
                    try:
                        self.cursor.execute("insert into task_message (task_id, pfw_attempt_id, message_time, message_lvl, message_pattern_id, message, log_file, log_line) values (%i, %i, TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF'), %i, %i, '%s', '%s', %i)"
                                            % (tid, self._pfwattid, self.search.findtime(text), self._patterns[self._indx]['lvl'], self._patterns[self._indx]['id'], self._message, self.fname, self.mlineno))
                        # commit the change in the case that the process dies, any error info may be saved first
                        self.cursor.execute("commit")
                        break
                    except:
                        # try reconnecting
                        self.reconnect()
                        # if two attempts have been made and failed then write a message to the log file
                        if i == 1:
                            if self._file:
                                self._lineno += 1
                                file.write(self, "QCF could not write the following to database:\n\t")
                # reset the message
                self._message = ""


def pfw_message(dbh, pfwattid, taskid, text, level):
    """ Method to provide direct access to the TASK_MESSAGE for pfwrunjob.py for custom error
        messages.

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
    sql = "insert into task_message (task_id, pfw_attempt_id, message_time, message_lvl, message_pattern_id, message, log_file, log_line) values (%i, %i, TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF'), %i, 0, '%s', '%s', %i)" \
                   % (int(taskid), int(pfwattid), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), level, text2, "pfwrunjob.py", 0)
    cursor.execute(sql)
    cursor.execute("commit")
