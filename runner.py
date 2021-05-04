import requests
import json
from datetime import datetime, timedelta

COWIN_URL = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/calendarByDistrict"
TELEGRAM_URL = "https://api.telegram.org/bot[BOT_API_KEY]/sendMessage"
DISTRICTS_IDS_TO_FETCH = [16, 5, 11, 12, 4, 9]
DISTRICTS_ID_CHANNEL_MAP = {
    16: '@u45WestGodavariAp',
    5: '@u45GunturAp',
    11: '@u45EastGodavariAp',
    12: '@u45PrakasamAp',
    4: '@u45KrishnaAp',
    9: '@u45AnantapurAp'
}
# 16 (West Godavari), 5 (Guntur), 11(East Godavari), 12(Prakasam), 4(Krishna), 9(Anantapur)

def _post_to_telegram(channel, message):
    data = {'chat_id': channel, 'text': message[:4096] if len(message) > 4096 else message}
    response = requests.request("POST", TELEGRAM_URL, headers={'content-type': 'application/json'}, data=json.dumps(data))
    print ("Response from ".format(response.text), flush=True)


def _cowin_call(dt, district_id):
    params = {
        'date': dt.strftime('%d-%m-%Y'),
        'district_id': district_id
    }
    response = requests.request("GET", COWIN_URL, headers={'content-type': 'application/json'}, data={}, params=params)
    if response.status_code/100 != 2:
        print("Error: response doe: {} response: {}".format(response.status_code, response.text), flush=True)
        return None
    return response.json()

def _get_address_from_center(center):
    return '{}, {}, {}, {} - {}'.format(center.get('name'), center.get('block_name'), center.get('district_name'), center.get('state_name'), center.get('pincode'))

def _process_cowin_slot_data(results):
    data = {}
    for center in results.get('centers'):
        if len(center.get('sessions', [])) == 0:
            continue
        for center_session in center['sessions']:
            if center_session.get('min_age_limit') < 45 and center_session.get('available_capacity') > 0:
                key = _get_address_from_center(center)
                if key not in data:
                    data[key] = []
                data[_get_address_from_center(center)].append(
                    {
                    'available_capacity': center_session.get('available_capacity'), 'vaccine': center_session.get('vaccine'),
                    'date': center_session.get('date')
                    })
    return data

def _send_to_appriopriate_channel(district_id, data):
    message = 'Vaccination centers for 18-44 group:\n'
    center_count = 1
    for key in data.keys():
        slots = data.get(key)
        center_msg = '{}. {}\n'.format(center_count, key)
        for slot in slots:
            center_msg = center_msg + '{} {} slots are available on {}\n'.format(slot.get('available_capacity'), slot.get('vaccine'),
                datetime.strptime(slot.get('date'), '%d-%m-%Y').strftime('%B %d'))
        message = message + center_msg + '\n'
        center_count = center_count + 1
    channel = DISTRICTS_ID_CHANNEL_MAP.get(district_id)
    _post_to_telegram(channel, message)

            


def run():
    print ("Start: {}".format(datetime.now()), flush=True)
    dt = datetime.now() + timedelta(minutes=330)
    for district_id in DISTRICTS_IDS_TO_FETCH:
        raw_data = _cowin_call(dt, district_id)
        if not raw_data:
            print ("No raw data")
            continue
        processed_data = _process_cowin_slot_data(raw_data)
        if not processed_data:
            print ("No processed data")
            continue
        _send_to_appriopriate_channel(district_id, processed_data)
    print ("End: {}".format(datetime.now()), flush=True)

if __name__ == "__main__":
    run()



