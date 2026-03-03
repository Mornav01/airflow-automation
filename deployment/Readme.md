# De-Identification Portal Setup Guide

## Step 1: Pull the Docker Image
### Option 1: Pull from DockerHub
```bash
docker pull deidentification_portal:1.0.0
```

### Option 2: Load from a ZIP File
```bash
docker load < deidentification_portal:1.0.0.zip
```

---

## Step 2: Set Up the `.env` File

- Use the provided `.env` file without making any changes for now.

---

## Step 3: Configure the `de-identification.yml` Compose File

- Update the `.env` file path in the `de-identification.yml` file if necessary.

---

## Step 4: Start the Docker Container

```bash
docker-compose -f de-identification.yml up -d
```

---

## Step 5: Account Creation Setup

### Access Jupyter Notebook
- Jupyter Notebook is running on `localhost:9888`
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

# Create account
## Set the permissions according to need
permissions = {
    "AddDataBase": {
        "has_permission": True
    },
    "UploadPHIConfigCSV": {
        "has_permission": True
    },
    "UpdatePHIConfig": {
        "has_permission": True
    },
    "PHIMarkingCompletedTick": {
        "has_permission": True
    },
    "PHIMarkingVerifiedTick": {
        "has_permission": True
    },
    "TableQCTick": {
        "has_permission": True
    },
    "UnLockPHIMarkingDB": {
        "has_permission": True
    },
    "LockPHIMarkingDB": {
        "has_permission": True
    },
    "StartDeIdentificationButton": {
        "has_permission": True
    },
}

user = create_account(username="", password="", permissions=permissions)
```

---

## Step 6: Log In to the Portal

- Access the portal at `http://localhost:8002/login`

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

