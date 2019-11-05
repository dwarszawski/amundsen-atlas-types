# Amundsen Atlas Types
[![PyPI version](https://badge.fury.io/py/amundsenatlastypes.svg)](https://badge.fury.io/py/amundsenatlastypes)
[![Build Status](https://api.travis-ci.org/dwarszawski/amundsen-atlas-types.svg?branch=master)](https://travis-ci.org/dwarszawski/amundsen-atlas-types)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](LICENSE)

Kickstart your Apache Atlas to support Amundsen using the prebuilt functions and required entity definitions.  

## Installation:
The package is available on PyPi, which you can install using below. 

```bash
    pip install amundsenatlastypes
```

## Usage:

#### Connecting to Apache Atlas:
`amundsenatlastypes` uses environment variables to connect to Apache Atlas. 

Following are the environment variables need to be set in order to connect to 
Apache Atlas. 

```bash
- ATLAS_HOST                [default = localhost]
- ATLAS_PORT                [default = 21000]
- ATLAS_USERNAME            [default = admin]
- ATLAS_PASSWORD            [default = admin]
```

#### Kickstart Apache Atlas
A single python function is available that you can use to apply all required entity definitions. 
You can run this function as many times as you want, and it will not break any existing functionality, that means
that it can also be implemented in your pipelines. 

```python
from amundsenatlastypes import Initializer
    
init = Initializer()
init.create_required_entities()
```

There also is a functionality to initiate your existing data to work accordingly with Amundsen. 
To create required relations you need to set `fix_existing_data=True` while calling the `create_required_entities()`.

```python
from amundsenatlastypes import Initializer
    
init = Initializer()
init.create_required_entities(fix_existing_data=True)
```


#### Functionality:
`amundsenatlastypes` provides a number of functions that can be used separately to 
implement/apply entity definitions of Apache Atlas, which are available [here](/amundsenatlastypes/__init__.py).


You can also simply access the individual entity definitions in JSON format by importing them 
from [here](amundsenatlastypes/types.py).  



