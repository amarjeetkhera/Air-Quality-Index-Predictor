# Importing Libraries
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from timezonefinder import TimezoneFinder
from datetime import date, datetime, timedelta
import time
import requests

# Defining cities and their coordinates
cities = {
    "Washington": [38.9072, -77.0369],
    "Bogota": [4.6097, -74.0817],
    "London": [51.5085, -0.1257],
    "Berlin": [52.5244, 13.4105],
    "Paris": [48.8566, 2.3522],
    "Madrid": [40.4168, -3.7038],
    "Tokyo": [35.6895, 139.6917],
    "Beijing": [39.9042, 116.4074],
    "Moscow": [55.7558, 37.6173],
    "Cairo": [30.0444, 31.2357],
    "Mexico City": [19.4326, -99.1332],
    "Rio de Janeiro": [-22.9068, -43.1729],
    "Mumbai": [19.0760, 72.8777],
    "Sydney": [-33.8688, 151.2093]
}

# Setting start and end dates
start_date = date(2023,1,1)
end_date = date.today()

# Air Quality variables
hourly_variables = [
    "pm10", "pm2_5", "carbon_monoxide",
    "ozone", "sulphur_dioxide", "nitrogen_dioxide",
    "european_aqi"
]

# Setting up the Open-Meteo API client and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Base URL for the Air Quality API
url = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Initiating a list to store dataframes for each city
all_cities_data = []

# Getting data for each city
for city, coords in cities.items():
    latitude, longitude = coords

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": ",".join(hourly_variables),
        "timezone": "auto"
    }

    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        hourly = response.Hourly()

        current_city_hourly_data = {
            "date": pd.date_range(
                start = pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end = pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq = pd.Timedelta(seconds=hourly.Interval()),
                inclusive = "left"
            )
        }

        for i, var_name in enumerate(hourly_variables):
            current_city_hourly_data[var_name] = hourly.Variables(i).ValuesAsNumpy()

        
        city_df = pd.DataFrame(data=current_city_hourly_data)
        city_df['city'] = city
        city_df['latitude'] = latitude
        city_df['longitude'] = longitude
        all_cities_data.append(city_df)

    except requests.exceptions.RequestException as e:
        print(f"  Error fetching data for {city}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"  API Error details: {e.response.text}")
    except Exception as e:
        print(f"  An unexpected error occurred for {city}: {e}")

    # Adding delay for API Limits
    time.sleep(1.5)

# Concatenating all dataframes
if all_cities_data:
    final_dataframe = pd.concat(all_cities_data).reset_index(drop=True)
    print(f"Total records collected: {len(final_dataframe)}")
    print(final_dataframe.head())
    print(f"\nDataFrame shape: {final_dataframe.shape}")
    final_dataframe.info()
else:
    print("\nNo data was collected. Check for errors during API calls.")

# Saving the dataframe as CSV
final_dataframe.to_csv("historical_air_quality_multi_city.csv", index=False)
