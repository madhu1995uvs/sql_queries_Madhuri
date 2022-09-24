# -*- coding: utf-8 -*-
"""
Created on Sun Jul 18 03:15:51 2021
@author: JCHAMBER
"""


def email_failed(subject=None, recipients='jchamber@childrensnational.org'):
    import win32com.client as win32
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = recipients
    mail.Subject = "Code failure"
    mail.Body = ''
    # mail.HTMLBody = '<h2>This is a Test email</h2>'  # this field is optional
    mail.HTMLbody = (f"{subject}")
    mail.Send()


def df_to_html(df):
    df = df.to_html(index=False, justify='center', border=3)
    df = df.replace('<tr>', '<tr align="center">')
    return df


def email_results_html(df, subject, date_range, recipients, mail_body='Data were abstracted at 06:00'):
    """
    Emails results of a dataframe in HTML format embedded in email
    Required arguments:
    df = dataframe
    date_range, usually a variable from the query but can be entered manually with surrounding quotes
    recipients, a list of email addresses separated by semicolon. Must be enclosed in quotes.
    mail_body, default = 'Data were extracted at 06:00'
    """
    import win32com.client as win32
    df = df.to_html(index=False, justify='center', border=3)
    df = df.replace('<tr>', '<tr align="center">')
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = recipients
    mail.Subject = f"{subject} for {date_range}"
    body = df
    body = mail_body + df
    start = """<html>
                <body> <br>
                    <strong>
                    </strong></br> <br>"""
    end = """</body><br>
           </html>
           """

    mail.HTMLBody = start + body + end
    mail.Send()


"""
def email_results_html(df,subject,date_range,recipients):
       import win32com.client as win32
       df = df.to_html(index = False, justify = 'center', border=3)
       df = df.replace('<tr>','<tr align="center">')
       outlook = win32.Dispatch('outlook.application')
       mail = outlook.CreateItem(0)
       mail.To = recipients
       mail.Subject = f"{subject} for {date_range}"
       body = df
       start =  < html >
                <body > <br >
                    <strong >
                    </strong > </br > <br > 
       end = < /body > <br >
           </html>
          
              
       mail.HTMLBody = start + body + end        
       mail.Send()
"""


def email_simple(subject, date_range, recipients, directory):
    import win32com.client as win32
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail = outlook.CreateItem(0)
    mail.To = f"{recipients}"
    mail.Subject = subject
    mail.HTMLbody = (f"""You have new {subject} data available in <br><br>
                        <a href='{directory}'>
                        {directory}</a>
                        """)
    mail.Send()


def email_with_attachment(subject, recipients, attachments, email_body='Data are attached'):
    """Sends an email with attachment(s).
    Required parameters:
    subject, enclosed in quotes
    recipients, a list of email addresses, separated by semicolon. List should be surrounded by quotes
    attachments, a list of filenames you are attaching, must include the path 
    email_body, enclosed in quotes. Default is 'Data are attached'
    """    
    import win32com.client as win32
    # Connects to your CNMC email
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = recipients
    mail.Subject = subject
    mail.Body = email_body
    for attachment in attachments:
        mail.Attachments.Add(attachment)
    mail.Send()

def validate_email(user_email):
    """Tries to send a test email to see if valid"""
    import win32com.client as win32
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = user_email
    mail.Subject = 'Test email'
    mail.HTMLbody = ("Just confirming your email address")
    try:
        mail.Send()
        print ('Username validated')
        email_valid = '1'
    except:
        print('The email failed. Please check that you entered a valid username')
        email_valid = '0'
    finally:
        pass
    return email_valid