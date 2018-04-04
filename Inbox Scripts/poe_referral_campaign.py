# This thing will use Inbox to send the Referral Promocodes to all participating users
import csv
import config
import requests
import json
import os
import time


def get_users(infile):
    users_promo_dict = {}
    reader = csv.DictReader(open(infile, 'rb'))
    for row in reader:
        uid = row['MetropianID']
        name = row['FIRST NAME']
        promocode = row['PROMOCODE']
        users_promo_dict[uid] = name, promocode

    return users_promo_dict


def check_last_user():
    if os.path.isfile('last_user.txt') == False:
        last_uid = 0
    else:
        text_file = open('last_user.txt', 'r')
        last_uid = text_file.readline()

    return int(last_uid)


def record_last_user(uid):
    # write the trip that we just processed
    f = open('last_user.txt', 'w')
    f.write(str(uid))


def write_to_csv(row):
    fields = ['userid', 'response code', 'response text']
    
    if os.path.isfile('poe_referral_log.csv') == False:
        with open("poe_referral_log.csv", "wb") as f:
            writer = csv.writer(f)
            writer.writerow(fields)
            writer.writerow(row)
    else:
        with open(r'poe_referral_log.csv', 'ab') as f:
            writer = csv.writer(f)
            writer.writerow(row)


def make_payload(user_id, values):
    payload = { 
        'to': {
            'uid': user_id
        },
		'title': 'Earn a $5 Gift Card!',
        'text': "Refer your friends to Metropia and you'll BOTH earn a $5 gift card!",
		'options': {
			'FNAME': values[0].upper(),
            'PROMOCODE': values[1]
            },
        'channel': channel_id
    }

    return json.dumps(payload)


def send_template(users_promo_dict):
    url = inbox_api + template_id
    last = check_last_user()
    for user, values in users_promo_dict.iteritems():
        if int(user) > last: # this makes sure we pick up from the last user we sent the message to
            payload = make_payload(user, values)
            response = requests.post(url, data=payload, headers={"Content-Type": "application/json"})
            if response.text != 'done':
                print (response.text, user)
            l_responses = [user, response.status_code , response.text]
            write_to_csv(l_responses)
            record_last_user(user)

    return 'finito!'


if __name__ == '__main__':
    print('starting POE Referral Send')
    start = time.time()
    env = 'production'  # sandbox or production
    inbox_api, channel_id, template_id = config.get_vars(env)
    print('ready to send templates', time.time() - start)
    users_dict = get_users('UpdatePROMOCODEelpaso.csv')
    send_template(users_dict)
    end = time.time()
    print('total script took', end - start)
    '''
    with open("final_output.csv", "wb") as f:
        writer = csv.writer(f)
        writer.writerows(responses)
    '''
