import asyncio
import aiohttp
import json
import os
from dotenv import load_dotenv
import logging
import time

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# API config
API_KEY = os.getenv('OPENWEATHER_API_KEY')
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# Delay conditions
DELAY_CONDITIONS = ['Rain', 'Snow', 'Thunderstorm', 'Drizzle', 'Extreme']


# ✅ Generate apology message
def generate_apology(order, weather_data):
    name = order['customer'].split()[0]
    city = order['city']
    weather_main = weather_data.get('weather', [{}])[0].get('main', 'unknown')
    weather_desc = weather_data.get('weather', [{}])[0].get('description', 'bad weather')

    return f"Hi {name}, your order (#{order['order_id']}) to {city} is delayed due to {weather_desc}. We appreciate your patience!"


# ✅ Fetch weather async
async def fetch_weather(session, city):
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }

    try:
        async with session.get(BASE_URL, params=params) as res:
            if res.status == 200:
                data = await res.json()
                logging.info(f"✅ Weather fetched for {city}")
                return {'city': city, 'success': True, 'data': data}
            else:
                logging.error(f"❌ Error for {city}")
                return {'city': city, 'success': False}
    except Exception as e:
        logging.error(f"❌ Exception for {city}: {e}")
        return {'city': city, 'success': False}


# ✅ Parallel processing
async def process_orders(orders):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_weather(session, order['city']) for order in orders]
        return await asyncio.gather(*tasks)


# ✅ Update orders
def update_orders(orders, results):
    weather_map = {r['city']: r['data'] for r in results if r['success']}

    delayed = []

    for order in orders:
        city = order['city']

        if city in weather_map:
            weather_main = weather_map[city]['weather'][0]['main']

            if weather_main in DELAY_CONDITIONS:
                order['status'] = 'Delayed'
                order['apology'] = generate_apology(order, weather_map[city])
                delayed.append(order)
            else:
                order['status'] = 'On Time'
        else:
            order['status'] = 'Error'

    return orders, delayed


# ✅ Save output
def save_orders(orders):
    with open('updated_orders.json', 'w') as f:
        json.dump(orders, f, indent=2)
    logging.info("📁 Saved updated_orders.json")


# ✅ Main function
async def main():
    logging.info("🚀 Starting Weather Project")

    if not API_KEY:
        logging.error("❌ API KEY missing")
        return

    try:
        with open('orders.json') as f:
            orders = json.load(f)
    except:
        logging.error("❌ orders.json missing")
        return

    results = await process_orders(orders)
    updated, delayed = update_orders(orders, results)

    save_orders(updated)

    logging.info(f"✅ Done! {len(delayed)} delayed orders")


# ✅ ENTRY POINT (IMPORTANT)
if __name__ == "__main__":
    asyncio.run(main())

    # Keep Render alive
    while True:
        print("Service running...")
        time.sleep(60)
