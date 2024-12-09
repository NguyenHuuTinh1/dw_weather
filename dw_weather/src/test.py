import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email():
    email = "huutinh2412@gmail.com"
    password = "tinh0394707535"  # Please remember to keep this secure and avoid hardcoding in production.
    email_sent = 'huutinh@tuoitrenonglam.com'

    session = smtplib.SMTP('smtp.gmail.com', 587)
    session.starttls()  # Enable security
    session.login(email, password)

    msg = MIMEMultipart()
    msg['From'] = email
    msg['To'] = email_sent
    msg['Subject'] = 'ALO ALO'  # Correct placement of the subject

    # Email body
    body = 'This is the content of the email.'
    msg.attach(MIMEText(body, 'plain'))

    session.sendmail(email, email_sent, msg.as_string())
    session.quit()  # Close the session

    print('Email sent!')


send_email()
