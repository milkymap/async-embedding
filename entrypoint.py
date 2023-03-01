import click 
import uvicorn

from loguru import logger 
from typing import List, Tuple, Dict, Optional 

from os import path 
from glob import glob
from time import perf_counter 

import multiprocessing as mp 

from server import start_server
from vectorizer import start_vectorizer

@click.group(chain=False, invoke_without_command=True)
@click.option('--transformers_cache', envvar='TRANSFORMERS_CACHE', required=True)
@click.pass_context
def command_line_interface(ctx:click.core.Context, transformers_cache:str):
    ctx.ensure_object(dict)
    ctx.obj['cache_folder'] = transformers_cache
    crr_command = ctx.invoked_subcommand
    if crr_command is not None:
        logger.debug(f'{crr_command} was called')

@command_line_interface.command()
@click.option('--port', type=int)
@click.option('--hostname', type=str)
@click.option('--model_name', type=str, default='all-mpnet-base-v2')
@click.option('--router_address', type=str, default='ipc://router.ipc')
@click.option('--publisher_address', type=str, default='ipc://publisher.ipc')
@click.pass_context
def start_services(ctx:click.core.Context, port:int, hostname:str, model_name:str, router_address:str, publisher_address:str):
    cache_folder = ctx.obj['cache_folder']
    server_process = mp.Process(
        target=start_server, 
        kwargs={
            'port': port, 
            'hostname': hostname, 
            'router_address': router_address, 
            'publisher_address': publisher_address
        }
    ) 
    
    vectorizer_process = mp.Process(
        target=start_vectorizer,
        kwargs={
            'model_name': model_name, 
            'cache_folder': cache_folder,
            'router_address': router_address, 
            'publisher_address': publisher_address
        }
    )

    try:
        vectorizer_process.start()
        server_process.start()

        vectorizer_process.join()
        server_process.join()
    except KeyboardInterrupt:
        logger.debug('ctl+c ...!') 
        vectorizer_process.join()
        server_process.join()
    except Exception as e:
        logger.error(e)
    finally:
        vectorizer_process.terminate()
        server_process.terminate()

if __name__ == '__main__':
    command_line_interface()