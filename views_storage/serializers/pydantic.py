
import pydantic
from views_storage.serializers import serializer

def pydantic_serializer(model: pydantic.BaseModel):

    class Serializer(serializer.Serializer):
        def serialize(self, obj: model)-> bytes:
            pass

        def deserialize(self, data: bytes)-> model:
            pass

    return Serializer
