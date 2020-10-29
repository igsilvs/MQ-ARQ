import requests as r
import pika, os
import time
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

def post(txt,msg):    
    resp = safe_request("http://127.0.0.1:5000/")
    if not resp:
        print("Servidor Indisponivel")
    else:
        url = os.environ.get('CLOUDAMQP_URL', 'amqps://eokiyulw:WdE9-aMSSrgfe9d_NRDcw4Kjr1M9Iktc@chimpanzee.rmq.cloudamqp.com/eokiyulw')
        params = pika.URLParameters(url)
        params.socket_timeout = 5

        connection = pika.BlockingConnection(params) # Connect to CloudAMQP
        channel = connection.channel() # start a channel
        channel.queue_declare(queue=txt) # Declare a queue
        # send a message
        channel.basic_publish(exchange='', routing_key=txt, body=msg)
        print ("[x] Message sent to consumer")
        connection.close()
        print("Mensagem enviada com sucesso para a fila " + str(txt))


def get(txt):
    def process_function(msg):
        print(f"{txt} processing")
        time.sleep(5) 
        print( "[x] Received " + str(msg))
    
    resp = safe_request("http://127.0.0.1:5000/")
    if not resp:
        print("Servidor Indisponivel")
    else:
        url = os.environ.get('CLOUDAMQP_URL', 'amqps://eokiyulw:WdE9-aMSSrgfe9d_NRDcw4Kjr1M9Iktc@chimpanzee.rmq.cloudamqp.com/eokiyulw')
        params = pika.URLParameters(url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel() 
        channel.queue_declare(queue=txt)

        def callback(ch, method, properties, body):
            process_function(body)

        # set up subscription on the queue
        channel.basic_consume(txt,
        callback,
        auto_ack=True)

        # start consuming (blocks)
        channel.start_consuming()
        connection.close()

def safe_request(url):
    try:
        retry_strategy = Retry(
        total=10,#tentativas para acessar servidor
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = r.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)
        http.get(url)
        return True
    except r.RequestException:
        return False
    
# post("hello","Hariel")
# get("hello")