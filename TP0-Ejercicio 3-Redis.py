import csv
import redis

# Connect to your Redis instance
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

rows = []
with open('bataxi.csv', 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rows.append(row)
            trip_id = row['id_viaje_r']
            longitude = float(row['origen_viaje_x'])
            latitude = float(row['origen_viaje_y'])

            # aadded data with geoadd
            r.geoadd('bataxi', (longitude, latitude, trip_id))

# Calculate the center point
center_lon = sum(float(row['origen_viaje_x']) for row in rows) / len(rows)
center_lat = sum(float(row['origen_viaje_y']) for row in rows) / len(rows)

# Query for all places within a radius of 100000 meters (100 km)
all_taxis = r.georadius('bataxi', center_lon, center_lat, 100000, unit='m')

#print(all_taxis)
#print(len(all_taxis))
#print(r.geopos('bataxi', "1"))

places = [
    {"place": "Parque Chas", "lon": -58.479258, "lat": -34.582497},
    {"place": "UTN", "lon": -58.468606, "lat": -34.658304},
    {"place": "ITBA Madero", "lon": -58.367862, "lat": -34.602938}
]

for place in places:
    # geoadd key longitude latitude place
   r.geoadd('places', (place["lon"], place["lat"], place["place"]))

center_lon = sum(place['lon'] for place in places) / len(places)
center_lat = sum(place['lat'] for place in places) / len(places)

all_places = r.georadius('places', center_lon, center_lat, 100000, unit='m', withcoord=False)

print("Data imported successfully!")

#print(all_places)

#Amount of taxis in a 1000 m radius from different locations
for i in range(0, len(all_places), 2):
    p = all_places[i + 1] #added for not double data showing
    place = r.geopos('places', p)
    p_lon, p_lat = place[0]
    amount = len(r.georadius('bataxi', p_lon, p_lat, 1000, withdist=True, withcoord=True))
    print(f"Place from {p} with total amount of taxi-rides: {amount} in 1 km radius")

# Get the number of keys in the Redis database
number_of_keys = r.dbsize()
print("Number of keys in the Redis database:", number_of_keys)

# Get the number of members in the 'bataxi' key
number_of_members = r.zcard('bataxi')
print("Number of members in the 'bataxi' key:", number_of_members)

