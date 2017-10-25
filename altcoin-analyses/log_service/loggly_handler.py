import logging
import logging.handlers

import socket
import traceback

from requests_futures.sessions import FuturesSession

session = FuturesSession()

def bg_cb(sess, resp):
    """ Don't do anything with the response """
    pass


class HTTPSHandler(logging.Handler):
    def __init__(self, url, fqdn=False, localname=None, facility=None):
        logging.Handler.__init__(self)
        self.url = url
        self.fqdn = fqdn
        self.localname = localname
        self.facility = facility

    # def get_full_message(self, record):
    #     if record.exc_info:
    #         return "\n".join(traceback.format_exception(*record.exc_info))
    #     else:
    #         return record.getMessage()

    def add_traceback(self, record):
        traceback_txt = ' '.join(traceback.format_exception(*record.exc_info))

        return traceback_txt.replace('"', "'")

    def alt_format(self, record):
        """
        Format the specified record as text.

        The record's attribute dictionary is used as the operand to a
        string formatting operation which yields the returned string.
        Before formatting the dictionary, a couple of preparatory steps
        are carried out. The message attribute of the record is computed
        using LogRecord.getMessage(). If the formatting string uses the
        time (as determined by a call to usesTime(), formatTime() is
        called to format the event time. If there is exception information,
        it is formatted using formatException() and appended to the message.
        """
        fmt = self.formatter
        record.message = record.getMessage()
        record.message = record.message.replace('"', "'")
        if fmt.usesTime():
            record.asctime = fmt.formatTime(record, fmt.datefmt)
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.add_traceback(record)
        try:
            s = fmt._fmt % record.__dict__
        except UnicodeDecodeError as e:
            # Issue 25664. The logger name may be Unicode. Try again ...
            try:
                record.name = record.name.decode('utf-8')
                s = fmt._fmt % record.__dict__
            except UnicodeDecodeError:
                raise e
        return s

    def emit(self, record):
        try:
            payload = self.alt_format(record)
            session.post(self.url, data=payload, background_callback=bg_cb)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)