import zmq 
import pickle

from time import sleep, perf_counter
from loguru import logger 
from sentence_transformers import SentenceTransformer

class ZMQVectorizer:
    def __init__(self, router_address:str, publisher_address:str, model_name:str, cache_folder:str):
        self.router_address = router_address
        self.publisher_address = publisher_address

        self.model_name = model_name 
        self.cache_folder = cache_folder
        self.zeromq_initialized = 0 

    def start_loop(self):
        if self.zeromq_initialized == 0:
            return 0 

        logger.debug('vectorizer starts the event loop')        
        keep_loop = True 
        while keep_loop:
            try:
                polled_status = self.router_socket.poll(100)  # 100ms 
                if polled_status == zmq.POLLIN:
                    client_address, _, encoded_text = self.router_socket.recv_multipart()
                    text = encoded_text.decode('utf-8')
                    embedding = self.vectorizer.encode(text, device='cpu')
                    self.router_socket.send_multipart([client_address, b''], flags=zmq.SNDMORE)
                    self.router_socket.send_pyobj(embedding)
            except KeyboardInterrupt:
                keep_loop = False 
                logger.warning('vectorizer get the ctl+c signal')
            except Exception as e:
                logger.error(e)
        # end while loop ...!

        logger.debug('vectorizer quits the event loop')
        self.publisher_socket.send(b'EXIT_LOOP', flags=zmq.SNDMORE)
        self.publisher_socket.send(b'')
        sleep(0.1)  # wait 10ms 

    def __enter__(self):
        try:
            self.vectorizer = SentenceTransformer(
                model_name_or_path=self.model_name,
                cache_folder=self.cache_folder,
                device='cpu'
            )
            logger.success('vectorizer loads the transforms')
        except Exception as e:
            logger.error(e)
        else:
            try:
                self.ctx = zmq.Context()
                self.router_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
                self.router_socket.bind(self.router_address)

                self.publisher_socket:zmq.Socket = self.ctx.socket(zmq.PUB)
                self.publisher_socket.bind(self.publisher_address)

                self.zeromq_initialized = 1 
                logger.success('vectorizer has finished to initialze zeromq ressources')
            except Exception as e:
                logger.error(e)

        return self 
    
    def __exit__(self, exc, val, traceback):
        if self.zeromq_initialized == 1:
            self.publisher_socket.close(linger=0)
            self.router_socket.close(linger=0)
            self.ctx.term()
            logger.success('vectorizer has reeased all zeromq ressources')
        logger.debug('vectorizer shutdown...!')

def start_vectorizer(model_name:str, cache_folder:str, router_address:str, publisher_address:str):
    with ZMQVectorizer(router_address, publisher_address, model_name, cache_folder) as agent:
        agent.start_loop()
