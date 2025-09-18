from google import genai
from google.genai.types import Tool, GenerateContentConfig, ThinkingConfig, GoogleSearch, UrlContext
import requests
from datetime import datetime, timedelta
import time
import psycopg
from dotenv import load_dotenv
import os

load_dotenv()

with psycopg.connect(f"dbname={os.getenv("DB_NAME")} host={os.getenv("HOST")}") as conn:
    with conn.cursor() as cur:
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS NEWS(
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    summary TEXT,
                    city VARCHAR(255) NOT NULL,
                    class VARCHAR(255) NOT NULL
                    )
                    """)
        cur.execute("""
                    CREATE INDEX IF NOT EXISTS index_news_city ON
                    NEWS(city)
                    """)




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

# cities = ["Kolkata"]

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

class_schema = {
  "type": "object",
  "properties": {
    "class": {
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  },
  "propertyOrdering": [
    "class"
  ],
  "required": [
    "class"
  ]
}

summary_schema = {
  "type": "object",
  "properties": {
    "res": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "index": {
            "type": "integer"
          },
          "summary": {
            "type": "string"
          },
          "classification": {
            "type": "string"
          }
        },
        "propertyOrdering": [
          "index",
          "summary",
          "classification"
        ],
        "required": [
          "index",
          "summary",
          "classification"
        ]
      }
    }
  },
  "propertyOrdering": [
    "res"
  ],
  "required": [
    "res"
  ]
}

cutoff_time = datetime.now() - timedelta(days=1)

news_data_dict = {}

search_tool = Tool(
    google_search = GoogleSearch()
)

context_tool = Tool(
    url_context=UrlContext()
)

api_keys = [os.getenv("GEMINI_API_KEY_1"),os.getenv("GEMINI_API_KEY_2")]
key_call_count = [0,0]

i = 1

def check_gemini_calls():
    global key_call_count
    if key_call_count[0] > 15 or key_call_count[1] > 15:
        print("Sleeping for 30 seconds")
        time.sleep(30)
        key_call_count = [0,0]

def relevance_of_titles(titles, exponential_count, retries):
  global i
  i = i ^ 1
  key_call_count[i]+=1
  client = genai.Client(api_key=api_keys[i])
  try:
    response = client.models.generate_content(
                model="gemini-2.5-flash-lite", contents=f"You are analysing news for a truck logistics business. I will provide a list of news titles. For each title, classify it as either Relevant or Irrelevant to trucking operations. When deciding relevance, consider factors such as transportation delays, traffic disruptions, road closures, strikes, weather warnings, fuel prices, regulations, and any other events that could affect truck logistics.\n Titles - \n{titles} \n Return index of only relevant titles and if titles are similar then only consider one.",
                config=GenerateContentConfig(thinking_config=ThinkingConfig(thinking_budget=-1), temperature=1, topP= 0.95, response_mime_type="application/json", response_schema=list_schema )
            )
    check_gemini_calls()
    return response.parsed
  except Exception as e:
    if retries > 0:    
      print(f"Error - {e}")
      print("Exponential_count",exponential_count)
      time.sleep(exponential_count)
      return title_to_summary(titles, exponential_count*2, retries-1)
    else:
      print("All retries failed.")
      return None


def title_to_summary(title, link, exponential_count, retries):
  global i
  i = i ^ 1
  key_call_count[i]+=1
  client = genai.Client(api_key=api_keys[i])
  try:
    response = client.models.generate_content(
            model="gemini-2.5-flash-lite", contents=f"Use the link and summarise the news. Link - {link} \n If you're unable to access the content, search the title to get the summary. Title - '{title}'",
            config=GenerateContentConfig(thinking_config=ThinkingConfig(thinking_budget=-1), temperature=1, topP= 0.95, tools=[context_tool,search_tool])
        )
    check_gemini_calls()
    return response.text
  except Exception as e:
    if retries > 0:    
      print(f"Error - {e}")
      print("Exponential_count",exponential_count)
      time.sleep(exponential_count)
      return title_to_summary(title, link, exponential_count*2, retries-1)
    else:
      print("All retries failed.")
      return None


def summary_classification(summary_str, exponential_count, retries):
    global i
    i = i ^ 1
    key_call_count[i]+=1
    client = genai.Client(api_key=api_keys[i])
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash-lite", contents=f"Analyse and rewrite the news provided. If news are similar then return only one. Also, classify these news by their potential impact on the truck logistics business. Use the categories high or low where high means major events that directly disrupt logistics (e.g., road closures, strikes, extreme weather, new transportation regulations, fuel price hikes) and low means minor events that could indirectly affect logistics (e.g., regional traffic updates, small-scale accidents, moderate weather warnings). \nNews -\n{summary_str}",
            config=GenerateContentConfig(thinking_config=ThinkingConfig(thinking_budget=-1), temperature=1, response_mime_type="application/json", response_schema=summary_schema)
        )
        check_gemini_calls()
        return response.parsed
    except Exception as e:
        if retries > 0:    
          print(f"Error - {e}")
          print("Exponential_count",exponential_count)
          time.sleep(exponential_count)
          return summary_classification(summary_str, exponential_count*2, retries-1)
        else:
          print("All retries failed.")
          return None


# Execution starts here

def main():
  print(time.ctime())
  exponential_count = 1
  retries = 5

  for city in cities:
      api_call_start = time.perf_counter()
      print(f"Current city - {city}")

      url = f"https://newsdata.io/api/1/latest?apikey={os.getenv("NEWS_DATA_API_KEY")}&qInTitle={city}&country=in&language=en"
      try:
        results = requests.get(url).json()
      except Exception as e:
        print(f"Error - {e}")
        continue
      
      if results['status'] == 'error':
          break

      data_arr = []

      for res in results["results"]:
          date_obj = datetime.strptime(res['pubDate'],"%Y-%m-%d %H:%M:%S")
          if date_obj >= cutoff_time:
              data_arr.append([res['title'],res['link']])

      if len(data_arr) > 0:
        titles = "\n".join([ f"{i}. {x[1]}" for i,x in enumerate(data_arr)])

        #only the relevant news titles are moved forward
        titles_response = relevance_of_titles(titles, exponential_count, retries)
        indexes = titles_response['index']
        print("indexes",indexes)

        if len(indexes) > 0:
          news_data_dict[city] = []
          filtered_news = []
          for index in indexes:
              data = data_arr[index]
              obj = {'title':data[0], 'link':data[1]}
              filtered_news.append(obj)
          
          for news in filtered_news:
              #gemini opens the link for the summary, if the link is broken then it searches the web for the summary
              summary_response = title_to_summary(news['link'],news['title'], exponential_count, retries)
              news['summary'] = summary_response

          summary_string = "\n".join([f"{i}. {x['summary']}" for i, x in enumerate(filtered_news)])
          #summaries are classified based on severity and the news are again filtered to remove duplicate news
          summary_class = summary_classification(summary_string, exponential_count, retries)

          for res in summary_class['res']:
            itr_indx = res['index']
            news_data_dict[city].append({'title':filtered_news[itr_indx]['title'], 'link':filtered_news[itr_indx]['link'], 'summary':res['summary'], 'class': res['classification']})

      api_call_end = time.perf_counter()
      elapsed_time = int(api_call_end - api_call_start)
      if(elapsed_time<30):
          print(f"sleeping for {30 - elapsed_time} seconds")
          time.sleep(30 - elapsed_time)

  if news_data_dict:
    news_data = []
    for city in news_data_dict:
        for data in news_data_dict[city]:
            print(data)
            news_data.append((data['title'], data['link'], data['summary'], city, data['class']))

    #news is pushed to db
    with psycopg.connect(f"dbname={os.getenv("DB_NAME")} host={os.getenv("HOST")}") as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                        INSERT INTO NEWS(title, link, summary, city, class)
                        VALUES(%s,%s,%s,%s,%s)
                        """,news_data)
  
if __name__ == "__main__":
   main()
   print(time.ctime())