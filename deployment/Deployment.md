# De-Identification Portal Setup Guide

## Step 1: Get the Docker Image
### Option 1: Load from a ZIP File
```bash
docker load < deidentification_portal:1.0.0.zip
```

### Option 2: Build Docker
#### Go to the folder where Dockerfile is present and run below command
```bash
docker build -t deidentification_portal:1.0.0 .
```

---

## Step 2: Set Up the `.env` File
### ENV file path : deployment/de-identification.env, define env values here


---

## Step 3: Define the setup_config.json

- define the users and there roles in this config

---

## Step 4: Start the Docker Container

```bash
docker-compose --env-file de-identification.env -f de-identification.yml up -d
```

---

## Step 5: Verify the deployment

### Access Jupyter Notebook
- Jupyter Notebook is running on `localhost:<NOTEBOOK-PORT>`
- Password: `ND@123`

---

### Add the Following Code in a Python Shell and Run It

```python
import os
import django
import sys

# Set up Django environment
sys.path.append('/Users/rohit.chouhan/NEDI/CODE/Dump/Project/DeIdentification/deIdentification')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from nd_api.models import DbDetailsModel, TableDetailsModel
from worker.models import Task, Chain
from django.contrib.auth.models import User
from nd_api.models.roles.rolemodel import RoleModel, get_default_permissions
from nd_scripts.create_account import create_account

def clean_db():
    RoleModel.objects.all().delete()
    User.objects.all().delete()
    DbDetailsModel.objects.all().delete()
    Chain.objects.all().delete()

```
---

## Step 6: Log In to the Portal

- Access the portal at `http://localhost:<SERVER-PORT>/login`

---

## Step 7: Add Database Details and Generate Stats

1. Add the database details in the portal.
2. Generate the stats for the data.

---

## Step 8: Set the Mapping DB Config

### Add the Following Code

```python
from nd_api.models import DbDetailsModel

db_model = DbDetailsModel.objects.get(
    db_name="<<your-database-name>>",
)
db_model.run_config["mapping_db_config"] = {
    "connection_str": "mysql+pymysql://root:123456789@localhost:3306/nddenttest_mapping",
    "inhouse_mapping_table": False,
}
db_model.save()
```

---

'mysql+pymysql://root:123456789@host.docker.internal:3306/full_automation'

