from airflow.utils.email import send_email_smtp

# Define email details
to = "ismailsamilacc@gmail.com"  # Replace with the recipient's email
subject = "Test Email from Airflow"
html_content = "<h3>This is a test email sent via Airflow SMTP</h3>"

# Send email
send_email_smtp(to, subject, html_content)

print("Test email sent!")
