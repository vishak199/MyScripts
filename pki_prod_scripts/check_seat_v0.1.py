import requests
import sys

api22 ='017d1f0298ca0b9c02_89F736A27B08E31A6919E8DC72F54DC69F5B53C6D0B0E7690564A50D6A4D976A'

url = "https://pki-ws-rest.symauth.com/mpki/api/v1/seat/"

#seatid = 'm9w0054g.houston.softwaregrp.net'

#SeatID retrived from SMAX
#common_name = "m9w0144g.houston.softwaregrp.net"
common_name = sys.argv[1]
seatid = common_name
#seatid = 'mc4w01851.itcs.softwaregrp.net'
certurl = url+seatid

headers = {

    'X-API-Key': api22,

    'Content-Type': "application/json"

    }

resp = requests.get(certurl, headers=headers)
print(resp.text)
print(resp.status_code)