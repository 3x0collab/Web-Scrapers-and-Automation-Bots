

from datetime import datetime as dt
import os
import sys
from email.mime.image import MIMEImage
# import django
# django.conf.settings.configure()

# from django.core.mail import EmailMultiAlternatives,get_connection
from email.mime.base import MIMEBase
from email import encoders
import premailer
import logging 
import datetime
import threading
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from AlertGRP.func import logError
from email.utils import make_msgid 


from email.message import EmailMessage

path = os.getcwd()
sys.path.append(path)

now = dt.now()
date = now.strftime('%d-%b-%y %H:%M:%S %p').upper()

BASE_DIR = Path(__file__).resolve().parent.parent

def attach_image(msg,f='',name=''):
    locn = os.path.abspath(os.path.dirname(__file__))
    file_location = os.path.join(BASE_DIR, f)
    print('IMAGES',file_location)
    fp = open(file_location, 'rb')
    msg_img = MIMEImage(fp.read())
    fp.close()
    msg_img.add_header('Content-ID', '<{}>'.format(name if name else f))
    msg.attach(msg_img)

def attach_excel_file(msg,f='',name=''):
    print('excel name',f)
    locn = f
    fp = open(locn[0], 'rb')
    part = MIMEBase('application', "octet-stream")
    part.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(part)    
    # locn = os.path.abspath(os.path.dirname(__file__))
    # file_location = os.path.join(locn,'excel')  
    name = locn[1] #.strip(file_location)
    part.add_header('Content-Disposition', 'attachment',filename=f'{name}')
    msg.attach(part)

def clean_emails(emails=[]):
    if emails:
        return [ x for x in list(set(emails)) if x]
    else:
        return []

# class EmailSync():
#     def __init__(self, subject, html_content, recipient_list=[], images=[], spf={}, xlsx=[]):
#         self.subject = subject
#         self.recipient_list = recipient_list
#         # self.recipient_list =  ['IbukunAkinteye@keystonebankng.com']

#         self.html_content = html_content
#         self.images = images
#         self.xlsx = xlsx  

#         print('herew2',spf)
#         self.connection = smtplib.SMTP(
#             host=spf.get("ricaStmpMailServer") ,
#             port= 587 #spf.get("ricaStmpMailPort") ,
#             )

#         self.from_email = spf.get("ricaStmpMailAddress") or spf.get("ricaStmpMailUser")  
#         # threading.Thread.__init__(self)

#     def start(self):
#         transformed_html = premailer.Premailer(self.html_content,cssutils_logging_level=logging.CRITICAL, disable_validation=True).transform()

#         to = []
#         cc = []
#         if isinstance(self.recipient_list,dict):
#             to = clean_emails(self.recipient_list.get('to',[])) 
#             cc = clean_emails( self.recipient_list.get('cc',[]))  
#         else:
#             to = clean_emails( self.recipient_list)


#         msg = EmailMessage(
#             self.subject, transformed_html, from_email=self.from_email,
#             to=to,cc=cc ,
#             connection=self.connection)

#         msg.content_subtype = "html"
#         msg.mixed_subtype = 'related' 

#         # for f in self.images:
#         #     attach_image(msg, f)
 
#         for f in self.xlsx:
#             attach_excel_file(msg, f)

#         print('ready to sent mail to ', self.recipient_list)


#         username=spf.get("ricaStmpMailUser") ,
#         password=spf.get("ricaStmpMailPassword") ,

#         self.connection.login(username, password)
#         self.connection.sendmail(from_email,to+cc, msg.as_string())

#         msg.send(fail_silently=False)
#         print('mail sent') 
    
#         self.connection.close()



def custom_send(subject, html_content, recipient_list=[], images=[], spf={},attachment=[],scenario_id=""):

    date_now = str(datetime.date.today()).replace("-","")
    time_now = datetime.datetime.now()
    current_time = str(time_now.strftime("%H:%M:%S")).replace(":","")

    log_args = {
    "ricaLogId":f'Mail-{date_now}-{current_time}',
    'ricaApplication':"Exception Mailing", 
    'ricaText':f"", 
    'ricaStatus':"",
    'ricaRunDate':date_now,
    'ricaRunTime':current_time,
   
    }



    from_email = spf.get("ricaStmpMailUser") or  spf.get("ricaAlertResponseFrom")  
    host=spf.get("ricaStmpMailServer") 
    port= 587 if 'ionos' in spf.get("ricaStmpMailServer") else  spf.get("ricaStmpMailPort")
    username=spf.get("ricaStmpMailUser")  
    password=spf.get("ricaStmpMailPassword")  
    # recipient_list = ['IbukunAkinteye@keystonebankng.com']

    to = []
    cc = []

    if isinstance(recipient_list,dict):
        to =   clean_emails(recipient_list.get('to',[])) 
        cc =   clean_emails(recipient_list.get('cc',[])) 

    else:
        to =  clean_emails(recipient_list)



    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject # 'Re: TEST1: TESTING QUERY THAT GROUP WITH INPUTTER, AUTHORISER ETC. ALERT ID: TEST1-005-20230910-14212355'
    msg['From'] = from_email
    msg['To'] = ','.join(to)
    msg['Cc'] = ','.join(cc) 
    msg['Reply-To'] = ','.join(to+cc) 
    # msg_id = make_msgid()
    # msg["Message-ID"] =  msg_id
    # msg["PR_IN_REPLY_TO_ID "] =  '<169427788812.2660.4096175939394910624@DESKTOP-3VNH0CA>'
    # msg["In-Reply-To"] =  '<169428159890.19492.732638349355495075@DESKTOP-3VNH0CA>'
    # msg["References"] =   '<169428159890.19492.732638349355495075@DESKTOP-3VNH0CA>'
    # # msg.add_header('PR_IN_REPLY_TO_ID', '<169428159890.19492.732638349355495075@DESKTOP-3VNH0CA>')
    # msg.add_header('In-Reply-To', '<846020099.2080102.1694291127712@email.ionos.co.uk>')
    # msg.add_header('References',  '<846020099.2080102.1694291127712@email.ionos.co.uk>')

    print('from_email',from_email)
    print('port',port)
    log_args['ricaSendTo'] = f"from:{from_email}, to:{','.join(to)}, cc:{','.join(cc)}"

    html  = html_content
    part2 = MIMEText(html, 'html')
    msg.attach(part2)

    for f in attachment:
        attach_excel_file(msg, f)
    
    try:
        print('send mail to', to,cc)
        service = smtplib.SMTP(host, int(port))
        service.connect(host, int(port))
        service.ehlo()

        if str(port)=='587' or str(port)=='465' :
            service.starttls()
            service.login(username, password)

        service.sendmail(from_email,to+cc, msg.as_string())
        service.quit()
        log_args['ricaStatus'] = "Success"
        log_args['ricaText'] = "Mail Sent Successfully"
        logError("OutMail").log(log_args,scenario_id)
        print('mail sent')

    except Exception as e:
        log_args['ricaStatus'] = "Success"
        log_args['ricaText'] = str(e)
        logError("OutMail").log(log_args,scenario_id)
        print('mail error', e)

def send_html_mail( subject, html_content, recipient_list,images,spf, xlsx,scenario_id=""): 
    # port = spf.get("ricaStmpMailPort")
    # if str(port) == '25':
    custom_send(subject, html_content, recipient_list, images, spf, attachment=xlsx,scenario_id=scenario_id)
    # else:            
    #     EmailSync(subject, html_content, recipient_list,images,spf, xlsx=xlsx).start()
