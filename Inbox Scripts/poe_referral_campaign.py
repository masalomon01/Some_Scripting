# This thing will use Inbox to send the Referral Promocodes to all participating users
import csv
import config
import requests
import json

def get_users(infile):
    users_promo_dict = {}
    reader = csv.DictReader(open(infile, 'rb'))
    for row in reader:
        uid = row['MetropianID']
        name = row['FIRST NAME']
        promocode = row['PROMOCODE']
        users_promo_dict[uid] = name, promocode

    return users_promo_dict


def make_payload(user_id, values):
    payload = { 
        'to': {
            'uid': user_id
        },
		'title': 'Your Referral Code',
        'text': 'Here you can find your referral code to share with your friends',
		'options': {
			'NAME': values[0],
            'PROMOCODE': values[1]
            },
        'channel': channel_id
    }

    return json.dumps(payload)


def send_template(users_promo_dict):
    url = inbox_api + template_id
    fields = ['userid', 'response code', 'response text']
    with open(r'output.csv', 'ab') as f:
        writer = csv.writer(f)
        writer.writerow(fields)
        for user, values in users_promo_dict.iteritems():
            payload = make_payload(user, values)
            response = requests.post(url, data=payload, headers={"Content-Type": "application/json"})
            if response.text != 'done':
                print response.text, user
            l_responses = [user, response.status_code , response.text]
            writer.writerow(l_responses)
    f.close()


if __name__ == '__main__':
    print('starting POE Referral Send')
    env = 'production'  # sandbox or production
    inbox_api, channel_id, template_id = config.get_vars(env)
    users_dict = get_users('POEPromocodes _partial.csv')
    send_template(users_dict)

    '''
    with open("final_output.csv", "wb") as f:
        writer = csv.writer(f)
        writer.writerows(responses)
    '''
