import os
import sqlite3
import requests
import json
from datetime import datetime, timedelta

conn = sqlite3.connect(os.environ['COWIN_DB_PATH'])

COWIN_URL = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/calendarByDistrict"
TELEGRAM_URL = "https://api.telegram.org/bot{}/sendMessage".format(os.environ['TELEGRAM_BOT_API_KEY'])
COWIN_BEARER_TOKEN = os.environ['COWIN_BEARER_TOKEN']
TEST_TELEGRAM_CHANNEL = '@betaguytest'
DISTRICTS_ID_CHANNEL_MAP = {
    16: '@u45WestGodavariAp',
    5: '@u45GunturAp',
    11: '@u45EastGodavariAp',
    12: '@u45PrakasamAp',
    4: '@u45KrishnaAp',
    9: '@u45AnantapurAp',
    294: '@u45BbmpKa',
    265: '@u45BbmpKa'
}
DISTRICTS_IDS_TO_FETCH = [294, 265, 16, 5, 11, 12, 4, 9]
# AP - 16 (West Godavari), 5 (Guntur), 11(East Godavari), 12(Prakasam), 4(Krishna), 9(Anantapur)
# KA - 294 (Bangalore BBMP), 265 (Bangalore Urban)

def _get_notified_slots_for_district(district_id):
    data = {}
    now = datetime.now() + timedelta(minutes=330)
    query = "select id, center_id, slot_date, age, slots from cowin_slots where district_id = {} and slot_date >= '{}'".format(district_id, now.strftime('%Y-%m-%d'))
    cursor = conn.execute(query)
    for row in cursor:
        pk = row[0]
        center_id = row[1]
        slot_date = row[2]
        age = row[3]
        slots = row[4]
        if center_id not in data:
            data[center_id] = {}
        data[center_id][slot_date] = {'id': pk, 'age': age, 'slots': slots}
    return data

def _upsert_slot_notification_details(notifcation_id, district_id, center_id, slot_date, age, available_slots):
    slot_date_str = slot_date.strftime('%Y-%m-%d')
    if notifcation_id:
        query = "update cowin_slots set slots = {} where id = {}".format(available_slots, notifcation_id)
    else:
        query = "insert into cowin_slots(center_id, district_id, slot_date, age, slots) values({}, {}, '{}', {}, {})".format(center_id, district_id, slot_date_str, age, available_slots)
    conn.execute(query)
    conn.commit()


def _post_to_telegram(channel, message):
    data = {'chat_id': channel, 'text': message[:4096] if len(message) > 4096 else message}
    response = requests.request("POST", TELEGRAM_URL, headers={'content-type': 'application/json'}, data=json.dumps(data))
    print ("Response from telegram status: {}, text: {}".format(response.status_code, "" if response.status_code/100 == 2 else response.text), flush=True)


def _cowin_call(dt, district_id):
    params = {
        'date': dt.strftime('%d-%m-%Y'),
        'district_id': district_id
    }
    headers = {'content-type': 'application/json', 'Cache-Control': 'no-cache',
    'origin': 'https://selfregistration.cowin.gov.in',
    'referer': 'https://selfregistration.cowin.gov.in/',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
    'authorization': 'Bearer {}'.format(COWIN_BEARER_TOKEN)
    }
    response = requests.request("GET", COWIN_URL, headers=headers, data={}, params=params)
    if response.status_code/100 != 2:
        print("Error: response doe: {} response: {}".format(response.status_code, response.text), flush=True)
        return None
    return response.json()

def _get_address_from_center(center):
    return '{}, {} - {}'.format(center.get('name'), center.get('block_name'), center.get('pincode'))

def _process_cowin_slot_data(results):
    data = {}
    for center in results.get('centers'):
        if len(center.get('sessions', [])) == 0:
            continue
        center_id = center.get('center_id')
        address = _get_address_from_center(center)
        for center_session in center['sessions']:
            if center_session.get('min_age_limit') < 45 and center_session.get('available_capacity') >= 3:
                if center_id not in data:
                    data[center_id] = {'address': address, 'slots': [], 'age': center_session.get('min_age_limit')}
                data[center_id]['slots'].append(
                    {
                    'available_capacity': center_session.get('available_capacity'), 'vaccine': center_session.get('vaccine'),
                    'date': center_session.get('date')
                    })
    return data

def _send_to_appriopriate_channel(district_id, data):
    message = ''
    center_count = 1
    already_notfied_slots_all_centers = _get_notified_slots_for_district(district_id)
    should_notify = None
    for center_id in data.keys():
        center_data = data.get(center_id)
        already_notfied_slots = already_notfied_slots_all_centers.get(center_id, {})
        age = center_data.get('age')
        slot_msg = ''
        for slot in center_data.get('slots'):
            slot_date = datetime.strptime(slot.get('date'), '%d-%m-%Y')
            notifcation = already_notfied_slots.get(slot_date.strftime('%Y-%m-%d'))
            notifcation_id = notifcation.get('id') if notifcation else None
            if notifcation and notifcation.get('slots') == slot.get('available_capacity'):
                # Already notified
                print ("Already notofied notifocation id: {} slot data: {}".format(notifcation.get('id'), slot), flush=True)
                continue
            slot_msg = slot_msg + '{} {} slots available on {}\n'.format(slot.get('available_capacity'), slot.get('vaccine'), slot_date.strftime('%B %d'))
            _upsert_slot_notification_details(notifcation_id, district_id, center_id, slot_date, age, slot.get('available_capacity'))
        if slot_msg:
            message = message + '{}\n'.format(center_count, center_data.get('address')) + slot_msg + '\n'
            center_count = center_count + 1
            should_notify = True
    if should_notify:
        channel = DISTRICTS_ID_CHANNEL_MAP.get(district_id)
        _post_to_telegram(channel, message)

def run():
    print ("Start: {}".format(datetime.now()), flush=True)
    dt = datetime.now() + timedelta(minutes=330)
    # print (_get_notified_slots_for_district(23))
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



