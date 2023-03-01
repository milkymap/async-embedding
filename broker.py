import zmq 
import threading

from dataclasses import dataclass
from loguru import logger 

@dataclass
class ZMQBrokerProperties:
    client_router_address:str 
    worker_router_address:str 
    client_publisher_address:str 
    worker_publisher_address:str 
    client_worker_pair_address:str 
    
class ZMQBroker:
    def __init__(self, address_config:ZMQBrokerProperties):
        self.address_config = address_config
        self.initialized = 0 
        self.main_thread_liveness = threading.Event()

    def client_handler(self):
        if self.initialized == 0:
            return 0 
        
        try:
            router_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
            router_socket.bind(self.address_config.client_router_address)

            publisher_socket:zmq.Socket = self.ctx.socket(zmq.PUB)
            publisher_socket.bind(self.address_config.client_publisher_address)

            pair_socket:zmq.Socket = self.ctx.socket(zmq.PAIR)
            pair_socket.connect(self.address_config.client_worker_pair_address)

            poller = zmq.Poller()
            poller.register(router_socket, zmq.POLLIN)
            poller.register(pair_socket, zmq.POLLIN)
        except Exception as e:
            logger.error(e) 
            return 0 

        logger.debug('client handler is up')

        keep_loop = True 
        while keep_loop and self.main_thread_liveness.is_set():
            logger.debug('client handler is up')

            try:
                map_socket2status = dict(poller.poll(100))
                if router_socket in map_socket2status:
                    if map_socket2status[router_socket] == zmq.POLLIN:
                        client_address, _, client_encoded_message = router_socket.recv_multipart()
                        pair_socket.send_multipart([client_address, b'', client_encoded_message])
                        
                if pair_socket in map_socket2status:
                    if map_socket2status[pair_socket] == zmq.POLLIN:
                        target_client_address, worker_response = pair_socket.recv_pyobj()
                        router_socket.send_multipart([target_client_address, b'', worker_response])
            except Exception as e:
                logger.error(e)
                keep_loop = False 
        # end while loop 

        publisher_socket.send(b'EXIT_LOOP', flags=zmq.SNDMORE) # topic 
        publisher_socket.send(b'') # empty message 

        poller.unregister(pair_socket)
        poller.unregister(router_socket)

        pair_socket.close(linger=0)
        router_socket.close(linger=0)
        publisher_socket.close(linger=0)
        logger.success('client handler has released all ressources')
        return 1 
    
    def worker_handler(self):
        if self.initialized == 0:
            return 0         
        try:
            router_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
            router_socket.bind(self.address_config.worker_router_address)

            publisher_socket:zmq.Socket = self.ctx.socket(zmq.PUB)
            publisher_socket.bind(self.address_config.worker_publisher_address)

            pair_socket:zmq.Socket = self.ctx.socket(zmq.PAIR)
            pair_socket.bind(self.address_config.client_worker_pair_address)

            poller = zmq.Poller()
            poller.register(router_socket, zmq.POLLIN)
            poller.register(pair_socket, zmq.POLLIN)
        except Exception as e:
            logger.error(e) 
            return 0 

        worker_pool = []

        logger.debug('worker handler is up')
        keep_loop = True 
        while keep_loop and self.main_thread_liveness.is_set():
            logger.debug('worker handler is up')

            try:
                map_socket2status = dict(poller.poll(100))
                if router_socket in map_socket2status:
                    if map_socket2status[router_socket] == zmq.POLLIN:
                        worker_address, _, msg_type, _, worker_encoded_message = router_socket.recv_multipart()
                        if msg_type == b'FREE':
                            worker_pool.append(worker_address)
                        if msg_type == b'DATA':
                            pair_socket.send(worker_encoded_message)  #(target_client_address, worker_response)
                
                if len(worker_pool) > 0:
                    if pair_socket in map_socket2status:
                        if map_socket2status[pair_socket] == zmq.POLLIN:
                            client_address, _, client_encoded_message = pair_socket.recv_multipart()
                            target_worker_address = worker_pool.pop(0)  # FIFO 
                            router_socket.send_multipart([target_worker_address, b''], flags=zmq.SNDMORE)
                            router_socket.send_pyobj((client_address, client_encoded_message))
            except Exception as e:
                logger.error(e)
                keep_loop = False 
        # end while loop 

        publisher_socket.send(b'EXIT_LOOP', flags=zmq.SNDMORE) # topic 
        publisher_socket.send(b'') # empty message 

        poller.unregister(pair_socket)
        poller.unregister(router_socket)

        pair_socket.close(linger=0)
        router_socket.close(linger=0)
        publisher_socket.close(linger=0)
        logger.success('worker handler has released all ressources')

    def start_services(self):
        client_thread = threading.Thread(target=self.client_handler)
        worker_thread = threading.Thread(target=self.worker_handler)

        try:
            client_thread.start()
            worker_thread.start()

            client_thread.join()
            worker_thread.join()
        except KeyboardInterrupt:
            logger.debug('main thread : ctl+c ...') 
        except Exception as e:
            logger.error(e)    
        finally:
            self.main_thread_liveness.clear()
            if client_thread.is_alive():
                client_thread.join()
            if worker_thread.is_alive():
                worker_thread.join()

    def __enter__(self):
        try:
            self.ctx = zmq.Context()
            self.ctx.set(zmq.MAX_SOCKETS, 6)
            self.main_thread_liveness.set()
            self.initialized = 1
        except Exception as e:
            logger.error(e) 
        return self 
    
    def __exit__(self, exc, val, traceback):
        if exc is not None:
            logger.warning(f'{exc} was raised...!')
            
        if self.initialized == 1:
            self.ctx.term()


if __name__ == '__main__':
    address_config = ZMQBrokerProperties(
        client_router_address='ipc://frontend_router.ipc',
        worker_router_address='ipc://backend_router.ipc',
        client_publisher_address='ipc://frontend_publisher.ipc',
        worker_publisher_address='ipc://backend_publisher.ipc',
        client_worker_pair_address='inproc://frontend_backend_interface'
    )

    try:
        with ZMQBroker(address_config=address_config) as broker:
            broker.start_services()
    except Exception as e:
        logger.error(e)