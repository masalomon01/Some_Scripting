# This thing will use Inbox to send the Referral Promocodes to all participating users
import csv
import requests
import json
import config


def get_users(infile):
    users_dict = {}
    reader = csv.DictReader(open(infile, 'rb'))
    for row in reader:
        uid = row['metropianid']
        name = row['First_Name']
        users_dict[uid] = name

    return users_dict


def make_payload(user_id, values):
    payload = { 
        'to': {
            'uid': user_id
        },
		'title': 'Win a $50 Amazon gift card',
        'text': "SURVEY: We're bringing exciting new transportation services to your area & we need YOUR HELP!",
		'options': {},
        'channel': channel_id
    }

    return json.dumps(payload)


def send_template(users_dict):
    url = inbox_api + template_id
    fields = ['userid', 'response code', 'response text']
    with open(r'output_amore.csv', 'ab') as f:
        writer = csv.writer(f)
        writer.writerow(fields)
        for user, values in users_dict.iteritems():
            payload = make_payload(user, values)
            response = requests.post(url, data=payload, headers={"Content-Type": "application/json"})
            if response.text != 'done':
                print (response.text, user)
            l_responses = [user, response.status_code , response.text]
            writer.writerow(l_responses)
    f.close()


if __name__ == '__main__':
    print('starting POE Referral Send')
    env = 'production'  # sandbox or production
    inbox_api, channel_id, template_id = config.get_vars(env)
    template_id = '5abbdfe06e2f2c0020a031d9'
    # users_dict = get_users('POEpromo_test.csv')  # test
    users_dict = get_users('amore_survey.csv')
    send_template(users_dict)

    '''
    with open("final_output.csv", "wb") as f:
        writer = csv.writer(f)
        writer.writerows(responses)
    '''
