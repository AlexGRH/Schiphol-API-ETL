# %%
import requests
import json
import mysql.connector
import configparser
import pandas as pd
import numpy as np

# read config file
config = configparser.ConfigParser(interpolation=None)
config.read('config.ini')


# helper function
def select_id(select, table, col, value):
  '''Selects the id from a table given a certain value and column'''
  try:
    cursor.execute(f"SELECT {select} FROM {table} WHERE {col} = {int(value)}")
  except:
    cursor.execute(f"""SELECT {select} FROM {table} WHERE {col} = "{value}";""")
  # return id
  try:
    return cursor.fetchone()[0]
  except:
    return 'null'

# connect to database
con = mysql.connector.connect(host=config['database']['host'], username=config['database']['username'], password=config['database']['pwd'], db=config['database']['db'], autocommit=True)
cursor = con.cursor()

url = "https://api.schiphol.nl/public-flights"

app_id='4db3813e'
app_key='a172bb92b7d530eaf9a7022d9afebd26'
Accept= 'application/json'
ResourceVersion= 'v4'
headers = {'app_id':app_id,'app_key':app_key,'accept':Accept,'resourceversion':ResourceVersion}


# aircraft
res=requests.get(url = url+'/aircrafttypes', headers=headers)
aircraftTypes = pd.DataFrame(json.loads(res.content)['aircraftTypes'])
# insert manufacturers
for iataMain in aircraftTypes['iataMain'].unique():
  cursor.execute(f"INSERT IGNORE INTO Manufacturers (iataMain) VALUES ('{iataMain}')")
# insert aircraftTypes
for i,row in aircraftTypes.iterrows():
  idManufacturer = select_id('idManufacturer','Manufacturers','iataMain',row['iataMain'])
  cursor.execute(f"""INSERT IGNORE INTO airCraftTypes 
  (iataSub,Manufacturers_idManufacturer, longDescription, shortDescription) 
  VALUES 
  ('{row['iataSub']}',{idManufacturer}, '{row['longDescription']}','{row['shortDescription']}')""")
  # check if an aircraftType has been inserted without name, and update with the name as well
  cursor.execute(f"SELECT longDescription FROM AircraftTypes WHERE Manufacturers_idManufacturer={idManufacturer} AND iataSub = '{row['iataSub']}'")
  if len(cursor.fetchall()) == 0:
    cursor.execute(f"UPDATE AircraftTypes set longDescription = '{row['longDescription']}', shortDescription= '{row['shortDescription']}' WHERE Manufacturers_idManufacturer={idManufacturer} AND iataSub = '{row['iataSub']}'")


  
  

# airlines
res=requests.get(url = url+"/airlines", headers=headers)
airlines = pd.DataFrame(json.loads(res.content)['airlines'])
# insert iata codes
for iata in airlines['iata'].unique():
  cursor.execute(f"INSERT IGNORE INTO Airline_IATA_codes (Airline_IATA_code) VALUES ('{iata}')")
# insert airlines
for i, row in airlines.iterrows():
  idIATA = select_id('idAirline_IATA_code','Airline_IATA_codes','Airline_IATA_code',row['iata'])
  cursor.execute(f"INSERT IGNORE INTO Airlines (Airline_IATA_codes_idAirline_IATA_code, ICAO, NVLS, publicName) VALUES ({idIATA},'{row['icao']}',{row['nvls']},'{row['publicName']}')")
  # check if an airline has been inserted for a flight already, and update with the name and nvls as well
  cursor.execute(f"SELECT publicName FROM Airlines WHERE Airline_IATA_codes_idAirline_IATA_code={idIATA} AND ICAO = '{row['icao']}'")
  if len(cursor.fetchall()) == 0:
    cursor.execute(f"UPDATE Airlines set publicName = '{row['publicName']}', NVLS= {row['nvls']} WHERE Airline_IATA_codes_idAirline_IATA_code={idIATA} AND ICAO = '{row['icao']}'")


# airports
res=requests.get(url = url+"/destinations", headers=headers)
destinations = pd.DataFrame(json.loads(res.content)['destinations'])
# insert countries
for country in destinations['country'].unique():
  cursor.execute(f'INSERT IGNORE INTO Countries (country) VALUES ("{country}")')
# insert cities
for i,row in destinations[['country','city']].drop_duplicates().iterrows():
  if row['country']:
    idCountry = select_id('idCountry','Countries','country',row['country'])
  else:
    idCountry = 'null'
  if row['city']:
    cursor.execute(f'INSERT IGNORE INTO Cities (Countries_idCountry,city) VALUES ({idCountry},"{row["city"]}")')
# insert airports
for i,row in destinations.iterrows():
  # retrieve idCountry
  if row['country']:
    idCountry = select_id('idCountry','Countries','country',row['country'])
  else:
    idCountry = 'null'
  # retrieve idCity, taking also the country into consideration
  if row['city']:
    cursor.execute(f'SELECT idCity FROM Cities WHERE Countries_idCountry = {idCountry} AND city = "{row["city"]}"')
    try:
      idCity = cursor.fetchone()[0]
    except:
      idCity = 'null'
  else:
    idCity = 'null'
  cursor.execute(f"""INSERT IGNORE INTO Airports 
  (IATA_code, Cities_idCity, publicName_dutch, publicName_english, Countries_idCountry)
  VALUES
  ("{row['iata']}",{idCity},"{row['publicName']['dutch']}","{row['publicName']['english']}",{idCountry})""")
  # check if aiport is already in database without name, if so, update
  cursor.execute(f"SELECT * FROM Airports WHERE IATA_code = '{row['iata']}'")
  if len(cursor.fetchall()) == 0:
    cursor.execute(f"""UPDATE Airports set 
    Cities_idCity={idCity},publicName_dutch="{row['publicName']['dutch']}",publicName_english="{row['publicName']['english']}",Countries_idCountry={idCountry} 
    WHERE IATA_code = '{row['iata']}'""")

# flights
res=requests.get(url = url+"/flights", headers=headers)
flights = pd.DataFrame(json.loads(res.content)['flights'])
for iata in flights['prefixIATA'].unique():
  cursor.execute(f"""INSERT IGNORE INTO Airline_IATA_codes (Airline_IATA_code) VALUES ('{iata}')""")
for i,row in flights[['prefixIATA','prefixICAO']].drop_duplicates().iterrows():
  idIata = select_id('idAirline_IATA_code','Airline_IATA_codes','Airline_IATA_code',row['prefixIATA'])
  cursor.execute(f"""INSERT IGNORE INTO Airlines (Airline_IATA_codes_idAirline_IATA_code, ICAO) VALUES ('{idIata}','{row['prefixICAO']}')""")
for i,row in flights.iterrows():
  # retrieve idAirline, taking the ICAO and overcoupling IATA into account
  cursor.execute(f"""SELECT idAirline FROM Airlines LEFT JOIN Airline_IATA_codes ON 
  Airline_IATA_codes_idAirline_IATA_code=idAirline_IATA_code WHERE ICAO = '{row['prefixICAO']}' AND Airline_IATA_code = '{row["prefixIATA"]}' """)
  idAirline = cursor.fetchone()[0]
  cursor.execute(f"INSERT IGNORE INTO flightNumbers (idflightNumber, Airlines_idAirline) VALUES ({row['flightNumber']}, {idAirline})")
  # insert individual destinations and connect them with the flightNumber
  for destination in row['route']['destinations']:
    cursor.execute(f"INSERT IGNORE INTO Airports (IATA_code) VALUES ('{destination}')")
    idAirport = select_id('idAirport','Airports','IATA_code',destination)
    cursor.execute(f"INSERT IGNORE INTO flightNumbers_has_Airports (flightNumbers_idflightNumber, Airports_idAirport) VALUES ({row['flightNumber']},{idAirport})")

  # insert aircrafttype in case it is not in the db yet
  cursor.execute(f"INSERT IGNORE INTO Manufacturers (iataMain) VALUES ('{row['aircraftType']['iataMain']}')")
  idManufacturer = select_id('idManufacturer','Manufacturers','iataMain',row['aircraftType']['iataMain'])
  cursor.execute(f"""INSERT IGNORE INTO airCraftTypes 
  (iataSub,Manufacturers_idManufacturer) 
  VALUES 
  ('{row['aircraftType']['iataSub']}',{idManufacturer})""")
  # retrieve idAircraftType, taking the manufacturer and iataSub into account
  cursor.execute(f"""SELECT idAircraftType FROM AircraftTypes LEFT JOIN Manufacturers ON 
  Manufacturers_idManufacturer=idManufacturer WHERE iataMain = '{row['aircraftType']['iataMain']}' AND iataSub = '{row["aircraftType"]['iataSub']}' """)
  idAircraftType = cursor.fetchone()[0]
  # insert the flight info update
  cursor.execute(f"""INSERT IGNORE INTO FlightInfo 
  (idFlightInfo, AircraftTypes_idAircraftType, flightNumbers_idflightNumber, scheduleDateTime, lastUpdatedAt, actualLandingTime, 
  actualOffBlockTime, estimatedLandingTime, flightDirection, publicEstimatedOffBlockTime)
  VALUES
  ({row['id']},{idAircraftType}, {row['flightNumber']},'{row['scheduleDateTime']}','{row['lastUpdatedAt']}','{row['actualLandingTime']}',
  '{row['actualOffBlockTime']}','{row['estimatedLandingTime']}','{row['flightDirection']}','{row['publicEstimatedOffBlockTime']}')""")

con.commit()

con.close()


