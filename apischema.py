from enum import Enum 
from pydantic import BaseModel

from typing import List, Tuple, Optional, Any, Callable

class ServerStatus(str, Enum):
    ALIVE:str='ALIVE'
    READY:str='READY'

class GenericResponseModel(BaseModel):
    status:bool 
    content:Any 
    error_message:str=None 

class EntrypointResponseContentModel(BaseModel):
    status:ServerStatus=ServerStatus.ALIVE 
    message:str

class EntrypointResponseModel(GenericResponseModel):
    content:Optional[EntrypointResponseContentModel]=None 


class ComputeEmbeddingRequestModel(BaseModel):
    text:str 

class ComputeEmbeddingResponseContentModel(BaseModel):
    embedding:List[float]=[]

class ComputeEmbeddingResponseModel(GenericResponseModel):
    content:Optional[ComputeEmbeddingResponseContentModel]=None 
