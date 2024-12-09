import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def send_email(subject, body):
    email = "huutinh.image@gmail.com"
    password = "btca betw bewk okrf"  # Please remember to keep this secure and avoid hardcoding in production.
    email_sent = 'huutinh@tuoitrenonglam.com'

    session = smtplib.SMTP('smtp.gmail.com', 587)
    session.starttls()  # Enable security
    session.login(email, password)

    msg = MIMEMultipart()
    msg['From'] = email
    msg['To'] = email_sent
    msg['Subject'] = subject  # Correct placement of the subject

    # Email body
    body = body
    msg.attach(MIMEText(body, 'plain'))
    session.sendmail(email, email_sent, msg.as_string())
    session.quit()  # Close the session
    print('Email sent!')


send_email("[ERROR] Craw data", f"URL hoặc đường dẫn file không hợp lệ \n Lỗi xuất hiện vào lúc {current_time}")

