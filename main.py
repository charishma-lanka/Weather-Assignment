import asyncio
import aiohttp
import json
import os
from dotenv import load_dotenv
import logging
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_check.log'),
        logging.StreamHandler()
    ]
)

# Get API key from environment
API_KEY = os.getenv('OPENWEATHER_API_KEY')
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# Weather conditions that cause delays
DELAY_CONDITIONS = ['Rain', 'Snow', 'Thunderstorm', 'Drizzle', 'Extreme']

# Weather-aware apology messages
def generate_apology(order, weather_data):
    """Generate personalized apology message based on weather"""
    
    customer_name = order['customer'].split()[0]  # Get first name
    city = order['city']
    weather_main = weather_data.get('weather', [{}])[0].get('main', 'unknown')
    weather_desc = weather_data.get('weather', [{}])[0].get('description', 'bad weather')
    temp_kelvin = weather_data.get('main', {}).get('temp', 0)
    temp_celsius = temp_kelvin - 273.15
    
    # Customize message based on weather type
    if weather_main == 'Rain':
        condition_msg = f"heavy {weather_desc}"
    elif weather_main == 'Snow':
        condition_msg = f"heavy snowfall"
    elif weather_main == 'Thunderstorm':
        condition_msg = f"severe thunderstorm"
    else:
        condition_msg = weather_desc
    
    # Generate personalized apology
    apology = f"Hi {customer_name}, your order (#{order['order_id']}) to {city} is delayed due to {condition_msg} ({temp_celsius:.1f}°C). We appreciate your patience! "
    
    return apology

async def fetch_weather(session, city):
    """Fetch weather data for a single city asynchronously"""
    
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'  # Celsius
    }
    
    try:
        async with session.get(BASE_URL, params=params) as response:
            if response.status == 200:
                data = await response.json()
                logging.info(f"✅ Successfully fetched weather for {city}")
                return {'city': city, 'success': True, 'data': data}
            elif response.status == 404:
                logging.error(f"❌ City not found: {city}")
                return {'city': city, 'success': False, 'error': 'City not found'}
            else:
                logging.error(f"❌ API error for {city}: Status {response.status}")
                return {'city': city, 'success': False, 'error': f'HTTP {response.status}'}
    
    except Exception as e:
        logging.error(f"❌ Exception for {city}: {str(e)}")
        return {'city': city, 'success': False, 'error': str(e)}

async def process_orders_concurrently(orders):
    """Process all orders concurrently using asyncio"""
    
    async with aiohttp.ClientSession() as session:
        # Create tasks for all cities
        tasks = []
        for order in orders:
            task = fetch_weather(session, order['city'])
            tasks.append(task)
        
        # Run all tasks concurrently
        results = await asyncio.gather(*tasks)
        
        return results

def update_orders_with_weather(orders, weather_results):
    """Update orders based on weather conditions"""
    
    # Create a mapping of city to weather result
    weather_map = {}
    for result in weather_results:
        if result['success']:
            weather_map[result['city']] = result['data']
    
    updated_orders = []
    delayed_orders = []
    
    for order in orders:
        city = order['city']
        
        # Check if weather data exists for this city
        if city in weather_map:
            weather_data = weather_map[city]
            weather_main = weather_data.get('weather', [{}])[0].get('main', '')
            
            # Check if weather condition causes delay
            if weather_main in DELAY_CONDITIONS:
                order['status'] = 'Delayed'
                order['delay_reason'] = weather_main
                order['weather_details'] = weather_data['weather'][0]
                order['apology_message'] = generate_apology(order, weather_data)
                delayed_orders.append(order)
                logging.info(f"🚚 Order {order['order_id']} to {city} DELAYED due to {weather_main}")
            else:
                order['status'] = 'On Time'
                order['weather_details'] = weather_data['weather'][0]
                logging.info(f"✅ Order {order['order_id']} to {city} ON TIME")
        else:
            # City not found or error - keep as pending but log
            order['status'] = 'Pending - Weather Check Failed'
            order['error'] = 'City not found or API error'
            logging.warning(f"⚠️ Order {order['order_id']} to {city} - Weather check failed")
        
        updated_orders.append(order)
    
    return updated_orders, delayed_orders

def save_orders_to_file(orders, filename='orders.json'):
    """Save updated orders to JSON file"""
    
    # Remove temporary fields if you want cleaner output
    output_orders = []
    for order in orders:
        output_order = {
            'order_id': order['order_id'],
            'customer': order['customer'],
            'city': order['city'],
            'status': order['status']
        }
        
        # Add optional fields if they exist
        if 'delay_reason' in order:
            output_order['delay_reason'] = order['delay_reason']
        if 'apology_message' in order:
            output_order['apology_message'] = order['apology_message']
        if 'error' in order:
            output_order['error'] = order['error']
        
        output_orders.append(output_order)
    
    with open(filename, 'w') as f:
        json.dump(output_orders, f, indent=2)
    
    logging.info(f"📁 Updated orders saved to {filename}")

def print_summary(delayed_orders):
    """Print summary of delayed orders with apology messages"""
    
    print("\n" + "="*60)
    print("📦 DELIVERY DELAY SUMMARY")
    print("="*60)
    
    if delayed_orders:
        for order in delayed_orders:
            print(f"\n🔴 Order #{order['order_id']} - {order['customer']}")
            print(f"   City: {order['city']}")
            print(f"   Reason: {order['delay_reason']}")
            print(f"   💬 {order['apology_message']}")
    else:
        print("\n✅ No delays detected! All orders on time.")
    
    print("\n" + "="*60)

async def main():
    """Main function to orchestrate the workflow"""
    
    logging.info("🚀 Starting Weather-Aware Delivery Delay System")
    
    # Check if API key is set
    if not API_KEY or API_KEY == 'your_api_key_here':
        logging.error("❌ API key not found! Please set OPENWEATHER_API_KEY in .env file")
        return
    
    # Load orders from JSON
    try:
        with open('orders.json', 'r') as f:
            orders = json.load(f)
        logging.info(f"📋 Loaded {len(orders)} orders from orders.json")
    except FileNotFoundError:
        logging.error("❌ orders.json file not found!")
        return
    except json.JSONDecodeError:
        logging.error("❌ Invalid JSON in orders.json!")
        return
    
    # Process weather data concurrently
    logging.info("🌤️ Fetching weather data for all cities concurrently...")
    weather_results = await process_orders_concurrently(orders)
    
    # Update orders based on weather
    updated_orders, delayed_orders = update_orders_with_weather(orders, weather_results)
    
    # Save updated orders to file
    save_orders_to_file(updated_orders)
    
    # Print summary
    print_summary(delayed_orders)
    
    # Log completion
    logging.info(f"✅ Processing complete! {len(delayed_orders)} orders delayed, {len(orders) - len(delayed_orders)} orders on time")

if __name__ == "__main__":
    asyncio.run(main())