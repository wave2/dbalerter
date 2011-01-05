# -*- coding: iso-8859-1 -*-
"""
    dbAlerter notification module

    @copyright: 2008-2011 Wave2 Limited. All rights reserved.
    @license: BSD License
"""

import os, smtplib, socket, sys, time, xmpp
import config
from email.MIMEImage import MIMEImage
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.Utils import parseaddr
from pkg_resources import resource_stream

__author__ = "Alan Snelson"
__copyright__ = "Copyright (c) 2008-2011 Wave2 Limited"

__revision__ = "$Id$"
__version__ = "0.1.0"

last_notification = time.time()
notification_count = 0

def initialise():
    """Initialise notification module"""

    pass

def notify (event_type, description, content=None):
    """Send Notifications"""

    global last_notification, notification_count
  
    # Throttle notifications to 15 a minute
    if (((time.time() - last_notification) < 60) and notification_count > 15):
        notification_count += 1
        return
    elif ((time.time() - last_notification) > 60): 
        notification_count = 0

    #Log entry
    log_notify(event_type, description)
  
    #XMPP Configured?
    if (config.has_option('dbAlerter', 'xmpp_to')):
        xmpp_notify (event_type, description, content)
  
    #SMTP Configured?
    if (config.has_option('dbAlerter', 'smtp_to')):
        smtp_notify (event_type, description, content)

    notification_count += 1
    last_notification = time.time()


def log_notify(event_type, description = '', destination=None):
    """Logfile access"""

    try:
        if (destination == 'Email' or destination == 'XMPP'):
            sys.stderr.write(time.strftime('%d-%m-%Y %H:%M:%S', time.localtime()) + ' - ' + destination + ': '+ event_type + ' - ' + description + "\n")
            sys.stderr.flush()
        elif (event_type == 'Startup'):
            sys.stdout.write ("\n" + time.strftime('%d-%m-%Y %H:%M:%S', time.localtime()) + ' - dbAlerter Daemon started with pid %d\n\n' % os.getpid() )
            sys.stdout.flush()
        elif (event_type == 'Shutdown'):
            sys.stdout.write ("\n" + time.strftime('%d-%m-%Y %H:%M:%S', time.localtime()) + ' - dbAlerter Daemon with pid %d Stopped\n' % os.getpid() )
            sys.stdout.flush()
        else:
            sys.stdout.write(time.strftime('%d-%m-%Y %H:%M:%S', time.localtime()) + " - Log: " + event_type + ' - ' + description + "\n")
            sys.stdout.flush()
    except IOError, ioe:
        sys.stdout.write(time.strftime('%d-%m-%Y %H:%M:%S', time.localtime()) + " - " + ioe.str + ' : ' + ioe.filename + "\n")
        notify("Error", ioe.str, ioe.str + ' : ' + ioe.filename)


def smtp_notify (event_type, description, content=None):
    """E-Mail Notification"""

    # Create the container (outer) email message.
    msg = MIMEMultipart('related')
    msg['From'] = config.get('dbAlerter','smtp_from')
    msg['To'] = config.get('dbAlerter','smtp_to')
    msg.preamble = 'Notification for event - ' + event_type

    # Guarantees the message ends in a newline
    msg.epilogue = ''

    # Encapsulate the plain and HTML versions of the message body in an
    # 'alternative' part, so message agents can decide which they want to display.
    msg_alt = MIMEMultipart('alternative')
    msg.attach(msg_alt)

    msg_text = MIMEText('This message was sent from the dbAlerter host (' + socket.gethostname() + ") to notify you that the following event has occured:\n\n" + event_type + ' - ' + description + "\n\nIf you are not the intended recipient of this notification please contact " + parseaddr(config.get('dbAlerter','smtp_from'))[1] + '.')
    msg_alt.attach(msg_text)

    if (content is None):
        msg['Subject'] = event_type + ' on ' + socket.gethostname()
        # We reference the image in the IMG SRC attribute by the ID we give it below
        msg_text = MIMEText('<a href="http://www.wave2.org/w2wiki/dbalerter"><img src="cid:dbalerterlogo"></a><p>This message was sent from the <a href="http://www.wave2.org/w2wiki/dbalerter">dbAlerter</a> host (<b>' + socket.gethostname() + '</b>) to notify you that the following event has occured:</p><b><i>' + event_type + '</i></b> - ' + description + '<p>If you are not the intended recipient of this notification please contact <b><a href="mailto:' + parseaddr(config.get('dbAlerter','smtp_from'))[1] + '">' + parseaddr(config.get('dbAlerter','smtp_from'))[1] + '</a></b>.', 'html')

    else:
        msg['Subject'] = event_type + ' - ' + description
        msg_text = MIMEText('<a href="http://www.wave2.org/w2wiki/dbalerter"><img src="cid:dbalerterlogo"></a><p>This message was sent from the <a href="http://www.wave2.org/w2wiki/dbalerter">dbAlerter</a> host (<b>' + socket.gethostname() + '</b>) with the following information:</p><pre>' + content + '</pre><p>If you are not the intended recipient of this notification please contact <b><a href="mailto:' + parseaddr(config.get('dbAlerter','smtp_from'))[1] + '">' + parseaddr(config.get('dbAlerter','smtp_from'))[1] + '</a></b>.', 'html')

    msg_alt.attach(msg_text)

    # This example assumes the image is in the current directory
    fp = resource_stream(__name__, 'images/dbAlerter.jpg')
    msg_image = MIMEImage(fp.read())
    fp.close()

    # Define the image's ID as referenced above
    msg_image.add_header('Content-ID', '<dbalerterlogo>')
    msg.attach(msg_image)
    # Send the email via our own SMTP server.
    s = smtplib.SMTP()
    s.connect(config.get('dbAlerter', 'smtp_server'))
    s.sendmail(config.get('dbAlerter', 'smtp_from'), config.get('dbAlerter', 'smtp_to'), msg.as_string())
    s.close()
    log_notify(event_type, description, 'Email')

def stateful_notify(state, dict, key, severity, subject, content):
    """Stateful notification"""

    if state:
        if (dict.has_key(key) and dict[key] == 1):
            pass
        else:
            dict[key] = 1
            notify(severity, subject, content)
    else:
        if (dict.has_key(key) and (dict[key] == 2 or dict[key] == 0)):
            pass
        elif (dict.has_key(key) and dict[key] == 1):
            dict[key] = 2
            notify(severity, subject, content)


def xmpp_notify (event_type, description, content=None):
    """XMPP Notification"""

    #Ignore Statistics
    if (event_type == 'Stats'):
        return
    jid = xmpp.protocol.JID(config.get('dbAlerter', 'xmpp_from'))
    cl = xmpp.Client(jid.getDomain(), debug=[])
    conn = cl.connect()
    if not conn:
        log_notify('Error', 'XMPP Failed to connect.')
        sys.exit(1)
    auth = cl.auth(jid.getNode(), config.get('dbAlerter', 'xmpp_password'), resource=jid.getResource())
    if not auth:
        log_notify('Error', 'XMPP Failed to authenticate.')
        sys.exit(1)
    id = cl.send(xmpp.protocol.Message(config.get('dbAlerter', 'xmpp_to'), event_type + ' from dbAlerter on (' + socket.gethostname() + ') - ' + description))
    cl.disconnect()
    log_notify(event_type, description, 'XMPP')
