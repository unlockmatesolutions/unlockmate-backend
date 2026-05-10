from fastapi import FastAPI, APIRouter, HTTPException, Depends, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
import uuid
from datetime import datetime, timezone, timedelta
import bcrypt
import jwt
import json
import asyncio
import re
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

class LocalCursor:
    def __init__(self, docs):
        self.docs = docs

    def sort(self, key, direction):
        reverse = direction < 0
        self.docs.sort(key=lambda doc: doc.get(key, ""), reverse=reverse)
        return self

    async def to_list(self, length):
        return self.docs[:length]


class LocalCollection:
    def __init__(self, store, name, lock, path):
        self.store = store
        self.name = name
        self.lock = lock
        self.path = path

    def _matches(self, doc, query):
        return all(doc.get(key) == value for key, value in query.items())

    def _project(self, doc, projection):
        if projection and projection.get("_id") == 0:
            return {key: value for key, value in doc.items() if key != "_id"}
        return dict(doc)

    async def _persist(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.store, indent=2), encoding="utf-8")

    async def find_one(self, query, projection=None):
        async with self.lock:
            for doc in self.store.setdefault(self.name, []):
                if self._matches(doc, query):
                    return self._project(doc, projection)
        return None

    async def insert_one(self, doc):
        async with self.lock:
            self.store.setdefault(self.name, []).append(dict(doc))
            await self._persist()

    async def update_one(self, query, update):
        async with self.lock:
            for doc in self.store.setdefault(self.name, []):
                if self._matches(doc, query):
                    doc.update(update.get("$set", {}))
                    await self._persist()
                    return

    async def delete_one(self, query):
        async with self.lock:
            docs = self.store.setdefault(self.name, [])
            for index, doc in enumerate(docs):
                if self._matches(doc, query):
                    docs.pop(index)
                    await self._persist()
                    return

    def find(self, query, projection=None):
        docs = [
            self._project(doc, projection)
            for doc in self.store.setdefault(self.name, [])
            if self._matches(doc, query)
        ]
        return LocalCursor(docs)


class LocalDatabase:
    def __init__(self, path):
        self.path = path
        self.lock = asyncio.Lock()
        if path.exists():
            self.store = json.loads(path.read_text(encoding="utf-8"))
        else:
            self.store = {"imei_requests": [], "admins": []}

    def __getattr__(self, name):
        return LocalCollection(self.store, name, self.lock, self.path)


mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
client = None

if os.environ.get("USE_LOCAL_DB", "").lower() in {"1", "true", "yes"}:
    db = LocalDatabase(ROOT_DIR / "local_db.json")
else:
    from motor.motor_asyncio import AsyncIOMotorClient

    client = AsyncIOMotorClient(mongo_url, serverSelectionTimeoutMS=1000)
    db = client[os.environ['DB_NAME']]

app = FastAPI()
api_router = APIRouter(prefix="/api")
security = HTTPBearer()

JWT_SECRET = os.environ.get('JWT_SECRET', 'your-secret-key-change-in-production')
JWT_ALGORITHM = 'HS256'
HARDCODED_ADMIN_USERNAME = "admin"
HARDCODED_ADMIN_PASSWORD = "mohitsha11"


class IMEIRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    imei: str
    customer_name: Optional[str] = None
    customer_email: Optional[str] = None
    customer_phone: Optional[str] = None
    country: Optional[str] = None
    carrier_name: Optional[str] = None
    phone_model: Optional[str] = None
    service_type: str = "icloud"
    service_id: Optional[str] = None
    service_name: Optional[str] = None
    service_price: Optional[float] = None
    payment_invoice_id: Optional[str] = None
    payment_url: Optional[str] = None
    payment_status: Optional[str] = None
    status: str = "pending"
    submitted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class IMEIRequestCreate(BaseModel):
    imei: str
    customer_name: Optional[str] = None
    customer_email: Optional[str] = None
    customer_phone: Optional[str] = None
    country: Optional[str] = None
    carrier_name: Optional[str] = None
    phone_model: Optional[str] = None
    service_type: Optional[str] = None
    service_id: Optional[str] = None


class IMEIStatusResponse(BaseModel):
    imei: str
    status: str
    submitted_at: datetime
    updated_at: datetime
    service_type: Optional[str] = None
    service_name: Optional[str] = None
    service_price: Optional[float] = None
    country: Optional[str] = None
    carrier_name: Optional[str] = None
    phone_model: Optional[str] = None
    payment_url: Optional[str] = None
    payment_status: Optional[str] = None


class AdminLogin(BaseModel):
    username: str
    password: str


class AdminLoginResponse(BaseModel):
    token: str
    username: str


class UpdateStatusRequest(BaseModel):
    status: str


class Service(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    service_type: str = "icloud"
    description: Optional[str] = None
    price: Optional[float] = None
    badge: Optional[str] = None
    is_active: bool = True
    sort_order: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ServiceCreate(BaseModel):
    name: str
    service_type: str = "icloud"
    description: Optional[str] = None
    price: Optional[float] = None
    badge: Optional[str] = None
    is_active: bool = True
    sort_order: int = 0


def normalize_datetime_fields(doc, fields):
    for field in fields:
        if isinstance(doc.get(field), str):
            doc[field] = datetime.fromisoformat(doc[field])
    return doc


def create_nowpayments_invoice(imei_request: IMEIRequest):
    api_key = os.environ.get("NOWPAYMENTS_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="Payment gateway is not configured")
    if not imei_request.service_price or imei_request.service_price <= 0:
        return None

    frontend_url = os.environ.get("FRONTEND_URL", "http://localhost:3000").rstrip("/")
    price_amount = int(imei_request.service_price) if float(imei_request.service_price).is_integer() else imei_request.service_price
    payload = {
        "price_amount": price_amount,
        "price_currency": "usd",
        "order_id": imei_request.id,
        "order_description": re.sub(
            r"[^A-Za-z0-9 ._:-]",
            "",
            f"{imei_request.service_name or 'iPhone unlock service'} IMEI {imei_request.imei}"
        )[:120],
        "success_url": f"{frontend_url}/?payment=success&imei={imei_request.imei}",
        "cancel_url": f"{frontend_url}/?payment=cancel&imei={imei_request.imei}",
    }
    body = json.dumps(payload).encode("utf-8")
    invoice_request = urlrequest.Request(
        "https://api.nowpayments.io/v1/invoice",
        data=body,
        headers={
            "x-api-key": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 iPhoneUnlockService/1.0",
        },
        method="POST",
    )

    try:
        with urlrequest.urlopen(invoice_request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        detail = error.read().decode("utf-8") or "Payment gateway rejected the invoice request"
        logger.error("NOWPayments invoice error: %s", detail)
        raise HTTPException(status_code=502, detail=f"Payment gateway rejected the invoice request: {detail}")
    except URLError as error:
        logger.error("NOWPayments connection error: %s", error)
        raise HTTPException(status_code=502, detail="Payment gateway is currently unavailable")


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


@api_router.get("/")
async def root():
    return {"message": "iPhone Unlock Service API"}


@api_router.get("/services", response_model=List[Service])
async def get_public_services():
    services = await db.services.find({"is_active": True}, {"_id": 0}).sort("sort_order", 1).to_list(1000)
    return [normalize_datetime_fields(service, ["created_at", "updated_at"]) for service in services]


@api_router.post("/imei", response_model=IMEIRequest)
async def submit_imei(request: IMEIRequestCreate):
    if len(request.imei) != 15 or not request.imei.isdigit():
        raise HTTPException(status_code=400, detail="IMEI must be exactly 15 digits")

    existing = await db.imei_requests.find_one({"imei": request.imei}, {"_id": 0})
    if existing:
        raise HTTPException(status_code=400, detail="IMEI already submitted. Check status instead.")

    request_data = request.model_dump()
    if not request.service_id:
        raise HTTPException(status_code=400, detail="Please select a service")

    service = await db.services.find_one({"id": request.service_id, "is_active": True}, {"_id": 0})
    if not service:
        raise HTTPException(status_code=400, detail="Selected service is not available")
    service_type = service.get("service_type", "icloud")
    request_data["service_type"] = service_type
    request_data["service_name"] = service["name"]
    request_data["service_price"] = service.get("price")
    if not request_data["service_price"] or request_data["service_price"] <= 0:
        raise HTTPException(status_code=400, detail="Selected service does not have a valid price")

    if service_type not in {"icloud", "carrier"}:
        raise HTTPException(status_code=400, detail="Service type must be 'icloud' or 'carrier'")
    if not request.customer_email:
        raise HTTPException(status_code=400, detail="Email is required")
    if service_type == "carrier":
        if not request.country:
            raise HTTPException(status_code=400, detail="Country is required for carrier unlock")
        if not request.carrier_name:
            raise HTTPException(status_code=400, detail="Carrier name is required for carrier unlock")
        if not request.phone_model:
            raise HTTPException(status_code=400, detail="Phone model is required for carrier unlock")

    imei_obj = IMEIRequest(**request_data)
    invoice = None
    if imei_obj.service_price:
        invoice = create_nowpayments_invoice(imei_obj)
        if invoice:
            imei_obj.payment_invoice_id = str(invoice.get("id", ""))
            imei_obj.payment_url = invoice.get("invoice_url")
            imei_obj.payment_status = "waiting"

    doc = imei_obj.model_dump()
    doc['submitted_at'] = doc['submitted_at'].isoformat()
    doc['updated_at'] = doc['updated_at'].isoformat()

    await db.imei_requests.insert_one(doc)
    return imei_obj


@api_router.get("/imei/{imei}", response_model=IMEIStatusResponse)
async def check_imei_status(imei: str):
    if len(imei) != 15 or not imei.isdigit():
        raise HTTPException(status_code=400, detail="Invalid IMEI format")

    request_doc = await db.imei_requests.find_one({"imei": imei}, {"_id": 0})
    if not request_doc:
        raise HTTPException(status_code=404, detail="IMEI not found. Please submit your IMEI first.")

    if isinstance(request_doc['submitted_at'], str):
        request_doc['submitted_at'] = datetime.fromisoformat(request_doc['submitted_at'])
    if isinstance(request_doc['updated_at'], str):
        request_doc['updated_at'] = datetime.fromisoformat(request_doc['updated_at'])

    return IMEIStatusResponse(
        imei=request_doc['imei'],
        status=request_doc['status'],
        submitted_at=request_doc['submitted_at'],
        updated_at=request_doc['updated_at'],
        service_type=request_doc.get('service_type'),
        service_name=request_doc.get('service_name'),
        service_price=request_doc.get('service_price'),
        country=request_doc.get('country'),
        carrier_name=request_doc.get('carrier_name'),
        phone_model=request_doc.get('phone_model'),
        payment_url=request_doc.get('payment_url'),
        payment_status=request_doc.get('payment_status')
    )


@api_router.post("/admin/login", response_model=AdminLoginResponse)
async def admin_login(login: AdminLogin):
    if login.username != HARDCODED_ADMIN_USERNAME or login.password != HARDCODED_ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = jwt.encode(
        {
            "username": HARDCODED_ADMIN_USERNAME,
            "exp": datetime.now(timezone.utc) + timedelta(hours=24)
        },
        JWT_SECRET,
        algorithm=JWT_ALGORITHM
    )

    return AdminLoginResponse(token=token, username=HARDCODED_ADMIN_USERNAME)


@api_router.get("/admin/services", response_model=List[Service])
async def get_admin_services(payload: dict = Depends(verify_token)):
    services = await db.services.find({}, {"_id": 0}).sort("sort_order", 1).to_list(1000)
    return [normalize_datetime_fields(service, ["created_at", "updated_at"]) for service in services]


@api_router.post("/admin/services", response_model=Service)
async def create_service(service: ServiceCreate, payload: dict = Depends(verify_token)):
    if not service.name.strip():
        raise HTTPException(status_code=400, detail="Service name is required")
    if service.service_type not in {"icloud", "carrier"}:
        raise HTTPException(status_code=400, detail="Service type must be 'icloud' or 'carrier'")
    if service.price is not None and service.price < 0:
        raise HTTPException(status_code=400, detail="Price cannot be negative")

    service_obj = Service(**service.model_dump())
    doc = service_obj.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    doc['updated_at'] = doc['updated_at'].isoformat()
    await db.services.insert_one(doc)
    return service_obj


@api_router.delete("/admin/services/{service_id}")
async def delete_service(service_id: str, payload: dict = Depends(verify_token)):
    service_doc = await db.services.find_one({"id": service_id}, {"_id": 0})
    if not service_doc:
        raise HTTPException(status_code=404, detail="Service not found")
    await db.services.delete_one({"id": service_id})
    return {"message": "Service removed successfully"}


@api_router.get("/admin/payment-config")
async def get_payment_config(payload: dict = Depends(verify_token)):
    return {
        "provider": "nowpayments",
        "configured": bool(os.environ.get("NOWPAYMENTS_API_KEY"))
    }


@api_router.get("/admin/requests", response_model=List[IMEIRequest])
async def get_all_requests(payload: dict = Depends(verify_token)):
    requests = await db.imei_requests.find({}, {"_id": 0}).sort("submitted_at", -1).to_list(1000)

    for req in requests:
        if isinstance(req['submitted_at'], str):
            req['submitted_at'] = datetime.fromisoformat(req['submitted_at'])
        if isinstance(req['updated_at'], str):
            req['updated_at'] = datetime.fromisoformat(req['updated_at'])

    return requests


@api_router.patch("/admin/requests/{request_id}", response_model=IMEIRequest)
async def update_request_status(request_id: str, update: UpdateStatusRequest, payload: dict = Depends(verify_token)):
    if update.status not in ["pending", "done"]:
        raise HTTPException(status_code=400, detail="Status must be 'pending' or 'done'")

    request_doc = await db.imei_requests.find_one({"id": request_id}, {"_id": 0})
    if not request_doc:
        raise HTTPException(status_code=404, detail="Request not found")

    updated_at = datetime.now(timezone.utc).isoformat()
    await db.imei_requests.update_one(
        {"id": request_id},
        {"$set": {"status": update.status, "updated_at": updated_at}}
    )

    updated_doc = await db.imei_requests.find_one({"id": request_id}, {"_id": 0})
    if isinstance(updated_doc['submitted_at'], str):
        updated_doc['submitted_at'] = datetime.fromisoformat(updated_doc['submitted_at'])
    if isinstance(updated_doc['updated_at'], str):
        updated_doc['updated_at'] = datetime.fromisoformat(updated_doc['updated_at'])

    return IMEIRequest(**updated_doc)


@api_router.post("/admin/seed")
async def seed_admin(x_seed_token: Optional[str] = Header(default=None, alias="X-Seed-Token")):
    seed_token = os.environ.get("ADMIN_SEED_TOKEN")
    if not seed_token or x_seed_token != seed_token:
        raise HTTPException(status_code=403, detail="Admin seed is not authorized")

    existing_admin = await db.admins.find_one({"username": "admin"})
    if existing_admin:
        return {"message": "Admin already exists"}

    admin_username = os.environ.get("ADMIN_USERNAME", "admin")
    admin_password = os.environ.get("ADMIN_PASSWORD")
    if not admin_password:
        raise HTTPException(status_code=500, detail="ADMIN_PASSWORD is required to seed the admin user")

    hashed_password = bcrypt.hashpw(admin_password.encode('utf-8'), bcrypt.gensalt())
    admin_doc = {
        "username": admin_username,
        "password": hashed_password.decode('utf-8')
    }
    await db.admins.insert_one(admin_doc)
    return {"message": "Admin user created successfully"}


app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@app.on_event("shutdown")
async def shutdown_db_client():
    if client:
        client.close()
