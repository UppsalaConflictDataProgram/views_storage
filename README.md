
# Views Storage

This package contains various classes used for storage and retrieval of data
within views. The main class exposed by the package is
`views_storage.key_value_store.KeyValueStore`. This class is composed with
`views_storage.serializers.serializer.Serializer` and
`views_storage.backends.storage_backend.StorageBackend` subclasses to provide
storage in various formats using various backends.

## Example

```
from views_storage.key_value_store import KeyValueStore
from views_storage.backends.azure import AzureBlobStorageBackend
from views_storage.serializers.pickle import Pickle

my_storage = KeyValueStore(
      backend = AzureBlobStorageBackend(
         connection_string = "...",
         container_name = "..."),
      serializer = Pickle()
   )

my_object = ...

my_storage.store("key", my_object)
```
