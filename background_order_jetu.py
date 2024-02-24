import aiohttp
import asyncio
import json
import requests

cancelled_orders = []  # Assuming typo correction
graphql_url = 'https://elmrnhqzybgkyhthobqy.hasura.eu-central-1.nhost.run/v1/graphql'

def send_fcm_notification(token, title, body):
    server_key = 'AAAAIOvJNSk:APA91bEvKFJXZ6YGP1nKcsMVMgTT4qIgOFSGepBHli4FMiQOVEumHHhy9fQvhvNZ8E2cDOKIvCDwiJC25SEJKwz9yEW-Y2SlnS4T2KLcNYJfjaPyRxPELZHCcyWSqkqlAGJowr2oof_W'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'key=' + server_key,
    }
    payload = {
        'to': token,
        'notification': {
            'title': title,
            'body': body
        }
    }
    response = requests.post('https://fcm.googleapis.com/fcm/send', headers=headers, json=payload)
    

def save_cancelled_orders(data):
    with open('cancelled_orders.json', 'w') as f:
        json.dump(data, f)

# Function to load cancelled orders from a file
def load_cancelled_orders():
    try:
        with open('cancelled_orders.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
async def fetch_graphql_data(url, query):
    async with aiohttp.ClientSession(headers={'x-hasura-admin-secret': "DXU^lp#*mUp_3yJ6VsaWS*(0pmq)kvY'"}) as session:
        async with session.post(url, json={'query': query}, ssl=False) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                
                return None

def addExistOrder(driverId, orderId):
    for order in cancelled_orders:
        if order['driverId'] == driverId:
            order['ordersId'].append(orderId)
            break
    else:  # If driverId not found, add a new entry
        cancelled_orders.append({'driverId': driverId, 'ordersId': [orderId]})
    save_cancelled_orders(cancelled_orders)  # Save updates

def isSendedOrder(driverId, orderId):
    isExist = False
    for order in cancelled_orders:
        if order['driverId'] == driverId:
            if orderId in order['ordersId']:  # Correction from .contains() to in
                isExist = True
    return isExist

async def main():
    while True:
        try:
            while True: 
                online_drivers_graphql_query = '''
                    query {
                            jetu_drivers(where: {is_background: {_eq: true}, is_free: {_eq: true}}) {
                                id
                                lat
                                long
                                token
                                is_free
                                is_background
                            }
                            }
                '''
                result = await fetch_graphql_data(graphql_url, online_drivers_graphql_query)

                onlineDrivers = result['data']['jetu_drivers']
                if len(onlineDrivers) > 0:
                    for driver in onlineDrivers:
                        lat = driver['lat']
                        long = driver['long']

                        orders_graphql_query = f'''query{{
                                order_by_location(args: {{lat: {lat}, lon: {long}}}){{
                                    id,
                                    jetu_user{{
                                        id,
                                        name,
                                        
                                    }},
                                    cost,
                                    comment,
                                    created_at,
                                    status,
                                    point_a_lat,
                                    point_a_long,
                                    point_b_lat,
                                    point_b_long,
                                    point_a_address,
                                    point_b_address,
                                    jetu_service{{
                                        id,
                                        title
                                    }}
                                }}
                            }}'''
                        orderResult = await fetch_graphql_data(graphql_url, orders_graphql_query)
                        
                        driverOrders = orderResult['data']['order_by_location']
                        for driverOrder in driverOrders:
                            
                            driver_id = driver['id']
                            order_id = driverOrder['id']
                            isExist = isSendedOrder(driver_id, order_id)
                            if not isExist:
                                driver_token = driver['token']
                                addExistOrder(driver_id, order_id)
                                if(driver['is_free']==True and driver['is_background']==True):
                                    send_fcm_notification(driver_token,'Jetu Pro','Новый заказ!')
                                    print(f'Send to {driver_token} , orderId {order_id}')
                
                await asyncio.sleep(3)  # Wait for 3 seconds before the next iteration
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            print("Перезапуск основного цикла через 3 секунд...")
            await asyncio.sleep(3)
# Start the loop
asyncio.run(main())
