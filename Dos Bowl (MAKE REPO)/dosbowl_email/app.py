from sleeper_wrapper import League, User
import matplotlib.pyplot as plt
import pandas as pd
import json
from datetime import datetime
import os
import boto3
os.environ['MPLCONFIGDIR'] = '/tmp/matplotlib'

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email import encoders

s3 = boto3.client('s3')
ses = boto3.client('ses', region_name='us-east-2')

def yearly_standings(year, league_name, league_owner):

    # locate the desired league by name
    user = User(league_owner)
    leagues = user.get_all_leagues('nfl', year)
    league_id = None
    for league in leagues:
        if league['name'] == league_name:
            league_id = league['league_id']
            break
    league = League(league_id)

    # get standings as pandas df
    rosters = league.get_rosters()
    users = league.get_users()
    data = league.get_standings(rosters, users)
    df = pd.DataFrame(data, columns=['display_name', 'w', 'l', 'total_points'])

    # change names based on mapping. People change their usernames
    user_data = [
        ("BrandynWales", "Wales"),
        ("kentnelson7", "Nelson"),
        ("Martysods", "Soderberg"),
        ("browned out", "Olson"),
        ("Squad", "Day"),
        ("Team vuke", "Vukelich"),
        ("Kelce x Swift = M8", "Wensman"),
        ("JakeReinking", "Reinking"),
        ("Tommypaal", "Paal"),
        ("Ramdog Raw Dawgs", "Shaffer"),
        ("Cjs All Day", "Olson"),
        ("Ra Dog", "Wales"),
        ("Ram Doggy", "Day"),
        ]
    user_mapping = {user_id: display_name for user_id, display_name in user_data}
    df['display_name'] = df['display_name'].map(user_mapping)

    # format df
    df = df.drop(columns=['l'])
    df['w'] = df['w'].astype(int)
    df['total_points'] = df['total_points'].astype(int)

    return df

def dos_bowl_rundown(year, league_name, league_owner):
    year_last = year - 1

    dosbowl_2023 = yearly_standings(year_last, league_name, league_owner)
    dosbowl_2024 = yearly_standings(year, league_name, league_owner)

    combined = pd.concat([dosbowl_2023, dosbowl_2024], axis=0)

    # Group by 'display_name' and calculate the sum for 'w', 'l', and 'total_points'
    df_grouped = combined.groupby('display_name').agg({'w': 'sum', 'total_points': 'sum'}).reset_index()

    df_sorted = df_grouped.sort_values(by=['w', 'total_points'], ascending=[False, False]).reset_index(drop=True)

    df_sorted['rank'] = df_sorted.index + 1

    return df_sorted

def save_df_to_png(df, filename, rank, champ):

    fig, ax = plt.subplots(figsize=(df.shape[1] * 2, df.shape[0] * .4))  # Adjust size as needed
    ax.axis('tight')
    ax.axis('off')

    table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center', bbox=[0, 0, 1, 1])

    # Format the headers
    for (i, j), cell in table.get_celld().items():
        if i == 0:  # Header row
            cell.set_text_props(fontweight='bold', fontsize=12)
            cell.set_facecolor('lightgrey')
            cell.set_text_props(text=df.columns[j].upper())

    # Alternate row colors
    for i in range(1, len(df) + 1):  # Row index starts from 1 for data rows
        row_color = 'white' if i % 2 == 1 else '#F5F5F5'
        for j in range(len(df.columns)):
            table[(i, j)].set_facecolor(row_color)   


    if rank >= 5:
        # Highlight the top 4 rows in green
        for i in range(1, min(5, len(df) + 1)):
            for j in range(len(df.columns)):
                table[(i, j)].set_facecolor('#D3FFD3')

        # Highlight the top 2 rows in green
        for i in range(1, min(3, len(df) + 1)):
            for j in range(len(df.columns)):
                table[(i, j)].set_facecolor('#A3D9A5')


    else:
        # Highlight the top 5 rows in green
        for i in range(1, min(6, len(df) + 1)):
            for j in range(len(df.columns)):
                table[(i, j)].set_facecolor('#D3FFD3')

        # Highlight the top 2 rows in green
        for i in range(1, min(3, len(df) + 1)):
            for j in range(len(df.columns)):
                table[(i, j)].set_facecolor('#A3D9A5')

    # Highlight the row where display_name == champ in blue
    champ_row_index = None
    for i in range(1, len(df) + 1):
        if df.iloc[i - 1]['display_name'] == champ:  # Adjust index for zero-based df
            champ_row_index = i
            break
    if champ_row_index is not None:
        for j in range(len(df.columns)):
            table[(champ_row_index, j)].set_facecolor('#7393B3')  # blue color for champ


    plt.savefig(filename, bbox_inches='tight', pad_inches=0.1)
    plt.close()

def merged_to_csv(df_sorted, df_2nd_recent, previous_champ, s3_bucket):

    # Merge df_2nd_recent onto df_recent using a left join on the display_name column
    merged_df = pd.merge(df_sorted, df_2nd_recent, on='display_name', how='left', suffixes=('_recent', '_2nd_recent'))
    merged_df['rank_change'] = merged_df['rank_2nd_recent'] - merged_df['rank_recent']
    merged_df = merged_df.drop(columns=['w_2nd_recent', 'total_points_2nd_recent', 'rank_2nd_recent'])
    merged_df = merged_df.rename(columns={'w_recent': 'w', 'total_points_recent': 'total_points', 'rank_recent': 'rank'})
    
    # Define a function to apply the conditions
    def update_rank_change(value):
        if value == 0:
            return '-'
        elif value > 0:
            return f'+{value}'
        else:
            return value

    # Apply the function to the 'rank_change' column
    merged_df['rank_change'] = merged_df['rank_change'].apply(update_rank_change)
    merged_df = merged_df.drop(columns=['rank'])
    merged_df

    date = datetime.now().strftime("%Y%m%d")
    date = str(date)
    print(date)

    rank = df_sorted.loc[df_sorted['display_name'] == previous_champ, 'rank']
    rank = rank.values[0]

    local_file_path = f'/tmp/{date}_dosbowl_standings.png'
    
    save_df_to_png(merged_df, local_file_path, rank, previous_champ)


    s3_folder = 'png'
    s3_file_path = f'{s3_folder}/{date}_dosbowl_standings.png'
    s3.upload_file(local_file_path, s3_bucket, s3_file_path)

def send_email(sender_email, sender_name, recipient_email, subject, body_text):
    try:
        msg = MIMEMultipart('related')
        msg['Subject'] = subject
        msg['From'] = f'{sender_name} <{sender_email}>'
        msg['To'] = recipient_email
    
        # Create the HTML body with line breaks
        body_html = """<html>
            <head></head>
            <body>
              <p><strong>{}</strong></p>
            </body>
            </html>""".format(body_text.replace('\n', '<br>'))

        # Attach the body in HTML format
        msg.attach(MIMEText(body_html, 'html'))

        # Attach the most recent PNG file from the images folder
        latest_image_key = get_latest_image_from_s3()
        
        if latest_image_key:
            # Download the most recent image
            latest_image_data = download_image_from_s3(latest_image_key)
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(latest_image_data)
            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(latest_image_key)}"')
            msg.attach(attachment)


        # Send the email
        response = ses.send_raw_email(
            Source=sender_email,
            Destinations=[recipient_email],
            RawMessage={'Data': msg.as_string()}
        )
    
        print("Email sent! Message ID:", response['MessageId'])

    except Exception as e:
        print('Error sending email:', str(e))
        
    return ('email sent')

def get_latest_image_from_s3():

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=images_folder)
    files = response.get('Contents', [])
    
    # Filter only PNG files
    png_files = [file for file in files if file['Key'].endswith('.png')]
    
    if not png_files:
        return None
    
    # Sort by last modified date to get the most recent one
    latest_file = max(png_files, key=lambda x: x['LastModified'])
    
    return latest_file['Key']

# Function to download an image from S3
def download_image_from_s3(key):
    response = s3.get_object(Bucket=bucket_name, Key=key)
    return response['Body'].read()

def get_weeks():
    # Define the date of September 5th
    start_date = datetime(2024, 9, 5)
    
    # Get the current date
    current_date = datetime.now()
    
    # Calculate the difference in days
    days_passed = (current_date - start_date).days
    
    # Calculate the number of full weeks (rounded down)
    weeks_passed = days_passed // 7 + 1
    
    return weeks_passed

league_owner = "joelrday"
league_name = "Dos Bowl"
year = 2024
s3_bucket = 'dosbowl'
date = datetime.now().strftime("%Y%m%d")
date = str(date)
prefix = 'csv/' # csv folder
previous_champ = "Wales"

# get last weeks csv to calculate rank change 
response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
files = response.get('Contents', [])
files = sorted(files, key=lambda x: x['LastModified'], reverse=True)
most_recent_file = files[0]['Key']
s3.download_file(s3_bucket, most_recent_file, '/tmp/most_recent.csv')
df_last_week = pd.read_csv('/tmp/most_recent.csv')

bucket_name = 'dosbowl'
images_folder = 'png/'


def lambda_handler(event, context):

    # create updated csv
    df_this_week = dos_bowl_rundown(year, league_name, league_owner)
    
    # save updated csv into tmp file 
    local_file_path2 = f'/tmp/{date}_dosbowl_standings.csv'
    df_this_week.to_csv(local_file_path2, index=False)
    
    # save from tmp into s3
    s3_file_path2 = f'{prefix}{date}_dosbowl_standings.csv'
    s3.upload_file(local_file_path2, s3_bucket, s3_file_path2)
    
    merged_to_csv(df_this_week, df_last_week, previous_champ,  s3_bucket)
    
    weeks_passed = get_weeks()

    # Define the sender email and display name
    sender_email = "dosbowl@chalkjuice.com"
    sender_name = "DosBowl"  # Change this to your desired display name
    recipient_emails = ('joelday.business@gmail.com', 'ramseyshaffer@gmail.com', 'bwales21@yahoo.com')
    subject = f'Week {weeks_passed} Standings'
    body_text = 'Blue = Clinched Spot \nGreen = Projected Playoff Spot'

    for rec in recipient_emails:
        send_email(sender_email, sender_name, rec, subject, body_text)

    return {
        'statusCode': 200,
        'body': 'Email sent my man'
    }