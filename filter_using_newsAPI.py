from google import genai
from google.genai.types import Tool, GenerateContentConfig, ThinkingConfig, GoogleSearch, UrlContext
import requests
from datetime import datetime, timedelta
import time
import psycopg
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv
import os

load_dotenv()

with psycopg.connect(f"dbname={os.getenv("DB_NAME")} host={os.getenv("HOST")}") as conn:
    with conn.cursor() as cur:
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS NEWS(
                    id VARCHAR(255) PRIMARY KEY,
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    city VARCHAR(255) NOT NULL,
                    class VARCHAR(255) NOT NULL
                    )
                    """)

pool = ConnectionPool("dbname=py_gres host=localhost")


cities = [
  'Gurugram',
  'Bhiwandi',
  'Chennai',
  'Ahmedabad',
  '"New Delhi"',
  'Kolkata',
  'Bengaluru',
  'Varanasi',
  'Guwahati',
  'Siliguri',
  'Jaipur',
  'Hubli',
  'Hyderabad',
  'Bharatpur',
  '"Ghaziabad district"',
  'Agra',
  'Nagpur',
  'Sirsa',
  'Raipur',
  'Gorakhpur',
  'Jodhpur',
  'Ludhiana',
  'Jhajjar',
  'Mumbai',
  'Siwan',
  'Kanpur',
  'Noida',
  'Itanagar',
  'Ajmer',
  'Ambala',
  'Amravati',
  'Amritsar',
  'Aurangabad',
  'Balasore',
  'Vadodara',
  '"Bharuch district"',
  'Bathinda',
  'Bijapur',
  'Davangere',
  'Erode',
  'Hisar',
  'Jalandhar',
  'Junagadh',
  'Kolhapur',
  'Madurai',
  'Mandya',
  'Mangalore',
  'Meerut',
  'Mohali',
  'Moradabad',
  'Mysore',
  'Nagercoil',
  'Vijayanagara',
  'Nashik',
  'Navsari',
  'Panaji',
  'Patiala',
  'Pondicherry',
  'Rajahmundry',
  'Rajkot',
  'Delhi',
  'Salem',
  'Shimoga',
  'Solapur',
  'Sonipat',
  'Surat',
  'Thane',
  'Thrissur',
  'Tirupati',
  'Tiruchirappalli',
  'Udaipur',
  'Vellore',
  'Vijayawada',
  'Visakhapatnam',
  'Mathura',
  'Lucknow',
  'Bhopal',
  'Coimbatore',
  'Faridabad',
  'Hosur',
  'Pune',
  'Rudharpur',
  'Ennore',
  'Pithampur',
  'Hoshairpur',
  '"CHAK GUJRAN"',
  'SHINDE',
  'Pitampur',
  'Pantnagar',
  'Alwar',
  'Zahirabad',
  'Nasik',
  'Dewas',
  'Mysuru',
  'Sanand',
  '"Gr. Noida"',
  'Palwal',
  'Ballabhgarh',
  'Badhalawadi',
  'Bhiwadi',
  'Patna',
  'Kudghat',
  'Sodepur',
  'Howrah',
  '"Salt lake"',
  'Hooghly',
  'Allahabad',
  'Mahipalpur',
  '"Shastri nagar"',
  'Gurgaon',
  '"Jagat puri"',
  'Sitapur',
  'Firozabad',
  'Gonda',
  'Raebareli',
  'Barabanki',
  'Bahraich',
  '"Swaroop nagar"',
  'Chhatarpur',
  'Kushinagar',
  'Sangrur',
  'Jammu'
]

# cities = ["Noida","Shimla","Kolkata"]

schema = {
  "type": "object",
  "properties": {
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "summary": {
            "type": "string"
          },
          "classification": {
            "type": "string",
            "enum": [
              "high",
              "mid",
              "low",
              "irrelevant"
            ]
          }
        },
        "propertyOrdering": [
          "summary",
          "classification"
        ],
        "required": [
          "summary",
          "classification"
        ]
      }
    }
  },
  "propertyOrdering": [
    "items"
  ],
  "required": [
    "items"
  ]
}

list_schema = {
  "type": "object",
  "properties": {
    "index": {
      "type": "array",
      "items": {
        "type": "integer"
      }
    }
  },
  "propertyOrdering": [
    "index"
  ],
  "required": [
    "index"
  ]
}

enum_schema = {
  "type": "object",
  "properties": {
    "class": {
      "type": "string",
      "enum": [
        "high",
        "low"
      ]
    }
  },
  "propertyOrdering": [
    "class"
  ],
  "required": [
    "class"
  ]
}

cutoff_time = datetime.now() - timedelta(days=1)
dict = {}

search_tool = Tool(
    google_search = GoogleSearch()
)

context_tool = Tool(
    url_context=UrlContext()
)

api_keys = [os.getenv("GEMINI_API_KEY_1"),os.getenv("GEMINI_API_KEY_2")]
key_call_count = [0,0]

i = 1

def check_gemini_calls(keys_count):
    if (keys_count[0]!=0 and keys_count[0]/15 == 0) or (keys_count[1]!=0 and keys_count[1]/15 == 0):
        print("Resetting time.")
        time.sleep(60)
        key_call_count[0] = 0
        key_call_count[1] = 0

indx = 0
api_call_start = time.time()

while indx < len(cities):
    print(f"index of the cites is {indx}")

    city = cities[indx]
    url = f"https://newsdata.io/api/1/latest?apikey={os.getenv("NEWS_DATA_API_KEY")}&qInTitle={city}&country=in&language=en"
    results = requests.get(url).json()
    print(results)
    
    if results['status'] == 'error':
        api_call_end = time.time()
        elapsed_time = int(api_call_end - api_call_start)
        print(f"News api sleeping for {900 - elapsed_time} seconds")
        time.sleep(900 - elapsed_time)
        api_call_start = time.time()
        continue

    data_arr = []

    for res in results["results"]:
        date_obj = datetime.strptime(res['pubDate'],"%Y-%m-%d %H:%M:%S")
        if date_obj >= cutoff_time:
            data_arr.append([res['article_id'],res['title'],res['link']])

    if len(data_arr) == 0:
        indx+=1
        continue

    titles = "\n".join([ f"{i+1}. {x[1]}" for i,x in enumerate(data_arr)])
    print(titles)

    i = i ^ 1
    key_call_count[i]+=1
    client = genai.Client(api_key=api_keys[i])
    response = client.models.generate_content(
                model="gemini-2.5-flash-lite", contents=f"You are analysing news for a truck logistics business. I will provide a list of news titles. For each title, classify it as either Relevant or Irrelevant to trucking operations. When deciding relevance, consider factors such as transportation delays, traffic disruptions, road closures, strikes, weather warnings, fuel prices, regulations, and any other events that could affect truck logistics.\n Titles - \n{titles} \n Return index of only relevant titles and if titles are similar then only consider one.",
                config=GenerateContentConfig(thinking_config=ThinkingConfig(thinking_budget=-1), temperature=1, topP= 0.95, response_mime_type="application/json", response_schema=list_schema )
            )
    indexes = response.parsed['index']
    check_gemini_calls(key_call_count)

    if len(indexes) == 0:
        indx+=1
        continue

    filtered_news = []
    for index in indexes:
        data = data_arr[index-1]
        obj = {'article_id': data[0], 'title':data[1], 'link':data[2]}
        filtered_news.append(obj)

    for news in filtered_news:
        i = i ^ 1
        key_call_count[i]+=1
        client = genai.Client(api_key=api_keys[i])
        summary_response = client.models.generate_content(
                model="gemini-2.5-flash-lite", contents=f"Use the link and summarise the news. Link - {news['link']} \n If you're unable to access the content, search the title to get the summary. Title - '{news['title']}'",
                config=GenerateContentConfig(thinking_config=ThinkingConfig(thinking_budget=-1), temperature=1, topP= 0.95, tools=[context_tool,search_tool])
            )
        news['summary'] = summary_response.text
        check_gemini_calls(key_call_count)

        i = i ^ 1
        key_call_count[i]+=1
        client = genai.Client(api_key=api_keys[i])
        classification_response = client.models.generate_content(
            model="gemini-2.5-flash-lite", contents=f"Classify this news item by its potential impact on the truck logistics business. Use the categories high or low where high means major events that directly disrupt logistics (e.g., road closures, strikes, extreme weather, new transportation regulations, fuel price hikes) and low means minor events that could indirectly affect logistics (e.g., regional traffic updates, small-scale accidents, moderate weather warnings).\n News - \n{summary_response.text}",
            config=GenerateContentConfig(thinking_config=ThinkingConfig(thinking_budget=-1), temperature=1, response_mime_type="application/json", response_schema=enum_schema)
        )
        news['class'] = classification_response.parsed['class']
        check_gemini_calls(key_call_count)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                        INSERT INTO NEWS(id, title, link, summary, city, class)
                        VALUES(%s,%s,%s,%s,%s,%s)
                        """,(news['article_id'], news['title'], news['link'], news['summary'], city, news['class']))
        conn.commit()

    indx+=1

pool.close()