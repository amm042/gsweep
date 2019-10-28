# gsweep -- gmail auto archiver
# Alan Marchiori 2019
# modified from Google Gmail API sample code

from __future__ import print_function
import pickle
from collections import defaultdict
from pprint import pprint
import datetime
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from dateutil.tz import tzlocal, tzutc
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

from EmailCache import emailCache
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.modify',
          'https://www.googleapis.com/auth/gmail.labels'
          ]
def getHeaderVal(metadata, name):
    for h in metadata['payload']['headers']:
        if h['name'] == name:
            return h['value']
    return None
def getLabelNames(usrs):
    "return a dict of NAME: LABEL_ID"
    fds = usrs.labels().list(userId='me').execute()
    existing = {x['name']: x['id'] for x in fds['labels']}
    return existing
def checkCreateLabels(usrs):
    "ensure Old labels exist for the user and return the label_Id map"

    existing = getLabelNames(usrs)
    print("LABELS:")
    pprint(existing)

    categories = list(map(lambda x: x[9:],
                    filter(
                        lambda x: x.startswith('CATEGORY_'),
                        existing.values())))

    print("CATEGORIES:", categories)
    #generate labels with Old suffix
    lbls = [x.capitalize()+'Old' for x in categories]
    reqd = set(lbls)

    for clab in reqd-set(existing.keys()):
        print('CREATE', clab)
        label = usrs.labels().create(
            userId='me',
            body={
                'labelListVisibility': 'labelShow',
                'messageListVisibility': 'show',
                'name': clab
            }).execute()
        # grab created id
        existing[clab] = label['id']
    #check....
    existing = getLabelNames(usrs)
    assert len(reqd - set(existing.keys())) == 0, "failed to create labels!"

    return existing

def main():

    creds = None
    # The file tokean.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)

    userProfile = service.users().getProfile(
        userId='me').execute()
    userEmail = userProfile['emailAddress']

    thread_service = service.users().threads()
    with emailCache(
        address=userEmail,
        service=thread_service) as mc:

        label_map = checkCreateLabels(service.users())
        nextPageToken = None
        thread_num = 0
        stat = defaultdict(int)

        while True:
            # process mails
            print('PAGE: ', nextPageToken)
            result = thread_service.list(
                userId='me',
                labelIds=['INBOX'],
                pageToken=nextPageToken).execute()

            # metadat to extract
            pinfo= ['Delivered-To', 'From', 'Subject', 'Date']
            now = datetime.datetime.now(tz=tzlocal())
            old = now - datetime.timedelta(days=5)

            for m in result['threads']:
                thread_meta = mc.getMetadata(id=m['id'])

                # find date of most recent message on thread
                last_message = datetime.datetime(1978,1,1, tzinfo=tzlocal())

                msg_labels = set()
                for thread_message in thread_meta['messages']:
                    internal_date = datetime.datetime.fromtimestamp(
                        int(thread_message['internalDate'])/1000,
                        tz=tzlocal())
                    if internal_date > last_message:
                        last_message = internal_date

                    msg_labels |= set(thread_message['labelIds'])

                if 'Keep' not in msg_labels:
                    if last_message < old:
                        cats = list(filter(lambda x: x.startswith('CATEGORY_'), msg_labels))

                        # just remove from INBOX.
                        remove_labels = []

                        # map label name to label_id that should already exist!
                        # remove the CATEGORY_ prefix on the Old label.
                        add_labels = [label_map[x[9:].capitalize()+'Old'] for x in cats]

                        if 'INBOX' in msg_labels:
                            remove_labels += ['INBOX']

                        print(thread_num, '----OLD', m['id'],
                              last_message-now,
                              msg_labels,
                              'REMOVE:', remove_labels,
                              'ADD:', add_labels,
                              [getHeaderVal(thread_meta['messages'][0], x) for x in pinfo])

                        stat['old'] += 1
                        new_result = thread_service.modify(
                            userId='me',
                            id=m['id'],
                            body={
                                'removeLabelIds': remove_labels,
                                'addLabelIds': add_labels
                            }).execute()
                        mc.update(new_result['id'], new_result)
                    else:
                        print(thread_num, '-NOTOLD', m['id'], last_message-now)
                        stat['notold'] += 1

                thread_num += 1

            if 'nextPageToken' in result:
                nextPageToken = result['nextPageToken']

            else:
                print("No nextPageToken. Done.")
                pprint(result)
                break

    print(stat)

if __name__ == '__main__':
    main()
