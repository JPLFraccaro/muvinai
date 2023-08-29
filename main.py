from fastapi import FastAPI
from pymongo.mongo_client import MongoClient
from config import USER, PASSWORD, DATABASE_NAME
import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from bson.objectid import ObjectId

app = FastAPI()
uri = f"mongodb+srv://{USER}:{PASSWORD}@cluster0.sf15y.mongodb.net/{DATABASE_NAME}"

client = MongoClient(uri)
db = client.challenge_set
clientes = db.clientes
boletas = db.boletas
planes = db.planes
merchants = db.merchants


@app.get("/")
async def read_root():
    return {"message": "Bienvenido, puedes acceder a la documentación en /docs"}

# 1 GET RESUMEN
@app.get("/resumen/{selected_month_year}")
async def get_resumen(selected_month_year: str):
    selected_date = parse(selected_month_year, dayfirst=False, yearfirst=True)
    start_of_month = datetime.datetime(selected_date.year, selected_date.month, 1, 0)
    end_of_month = start_of_month + relativedelta(months=1)
    start_of_last_month = start_of_month - relativedelta(months=1)

    activos = clientes.count_documents({
        'fecha_vigencia': {'$gte': end_of_month},
        'last_subscription_date': {'$lt': end_of_month}
    })

    inactivos = clientes.count_documents({
        'fecha_vigencia': {'$gte': start_of_month, '$lt': end_of_month}
    })

    activos_mes_anterior = clientes.count_documents({
        'fecha_vigencia' : {'$gte' : start_of_month},
        'last_subscription_date' : {'$lt' : start_of_month}
    })
    porcentaje_activos = ((activos-activos_mes_anterior) / activos_mes_anterior) * 100

    inactivos_mes_anterior = clientes.count_documents({
        'fecha_vigencia' : {'$gte': start_of_last_month, '$lt': start_of_month}
    })
    porcentaje_inactivos = ((inactivos-inactivos_mes_anterior) / inactivos_mes_anterior) * 100

    altas = 0
    bajas = 0
    for documento in clientes.find():
        for evento in documento['history']:
            if start_of_month <= evento['date_created'] < end_of_month:
                if evento['event'] == 'alta':
                    altas += 1
                elif evento['event'] == 'baja':
                    bajas += 1

    altas_mes_anterior = 0
    bajas_mes_anterior = 0
    for documento in clientes.find():
        for evento in documento['history']:
            if start_of_last_month <= evento['date_created'] < start_of_month:
                if evento['event'] == 'alta':
                    altas_mes_anterior += 1
                elif evento['event'] == 'baja':
                    bajas_mes_anterior += 1
    porcentaje_altas = ((altas-altas_mes_anterior) / altas_mes_anterior) * 100
    porcentaje_bajas = ((bajas-bajas_mes_anterior) / bajas_mes_anterior) * 100
    

    res = {
        'activos': activos,
        'porcentaje_activos': porcentaje_activos,
        'altas': altas,
        'porcentaje_altas': porcentaje_altas,
        'bajas': bajas,
        'porcentaje_bajas': porcentaje_bajas,
        'inactivos': inactivos,
        'porcentaje_inactivos': porcentaje_inactivos,
    }
    return res

# 2 GET GRAFICO COBROS
@app.get('/grafico_cobros/{selected_month_year}')
async def get_grafico(selected_month_year: str):
    selected_date = parse(selected_month_year, dayfirst=False, yearfirst=True)
    start_of_month = datetime.datetime(selected_date.year, selected_date.month, 1, 0)
    end_of_month = start_of_month + relativedelta(months=1)
    cobros_data = {}

    cursor = boletas.find({
        'date_created': {
            '$gte': start_of_month,
            '$lt': end_of_month
        },
        'source': {'$in': ['checkout', 'checkout3', 'checkout_miclub', 'recurring_miclub', 'recurring_charges']}
    })

    for document in cursor:
        if document['source'] == 'checkout' or document['source'] == 'checkout3' or document['source'] == 'checkout_miclub':
            source = 'alta'
        else:
            source = 'recurrente'
        day = document['date_created'].day
        final_price = document['charges_detail']['final_price']

        if day not in cobros_data:
            cobros_data[day] = {'alta': 0, 'recurrente': 0}
        
        cobros_data[day][source] += final_price
    
    return cobros_data

# 3 GET TOTAL MENSUAL
@app.get('/total/{selected_month_year}')
async def get_total(selected_month_year: str):

    selected_date = parse(selected_month_year, dayfirst=False, yearfirst=True)
    start_of_month = datetime.datetime(selected_date.year, selected_date.month, 1, 0)
    start_of_last_month = start_of_month - relativedelta(months=1)
    if start_of_month == datetime.datetime(2023, 6, 1, 0):
        end_of_month = start_of_month + relativedelta(days=11)
    else:
        end_of_month = start_of_month + relativedelta(months=1)

    month_tickets = boletas.find({
        'date_created': {
            '$gte': start_of_month,
            '$lt': end_of_month
        },
        'source': {'$in': ['checkout', 'checkout3', 'checkout_miclub', 'recurring_miclub', 'recurring_charges']}
    })
    last_month_tickets = boletas.find({
        'date_created': {
            '$gte': start_of_last_month,
            '$lt': start_of_month
        },
        'source': {'$in': ['checkout', 'checkout3', 'checkout_miclub', 'recurring_miclub', 'recurring_charges']}
    })
    month_total = {
        'total' : 0,
        'alta' : 0,
        'recurrente' : 0,
    }
    last_month_total = {
        'total' : 0,
        'alta' : 0,
        'recurrente' : 0,

    }
    
    for document in month_tickets:
        if document['source'] == 'checkout' or document['source'] == 'checkout3' or document['source'] == 'checkout_miclub':
            source = 'alta'
        else:
            source = 'recurrente'
        
        final_price = document['charges_detail']['final_price']
        
        month_total[source] += final_price
        month_total['total'] += final_price

    for document in last_month_tickets:
        if document['source'] == 'checkout' or document['source'] == 'checkout3' or document['source'] == 'checkout_miclub':
            source = 'alta'
        else:
            source = 'recurrente'
        
        final_price = document['charges_detail']['final_price']
        
        last_month_total[source] += final_price
        last_month_total['total'] += final_price

    porcentaje_total = (month_total['total']-last_month_total['total']) / last_month_total['total'] * 100
    porcentaje_altas= (month_total['alta']-last_month_total['alta']) / last_month_total['alta'] * 100
    porcentaje_recurrentes = (month_total['recurrente']-last_month_total['recurrente']) / last_month_total['recurrente'] * 100
    response = {
        'month_total' : month_total['total'],
        'porcentaje_total' : porcentaje_total,
        'month_altas' : month_total['alta'],
        'porcentaje_altas' : porcentaje_altas,
        'month_recurrentes' : month_total['recurrente'],
        'porcentaje_recurrentes' : porcentaje_recurrentes,
    }
    return response

# 4 GET porcentajes
@app.get('/porcentajes/{selected_month_year}/{merchant_id}')
async def get_porcentajes(selected_month_year: str, merchant_id: str):

    selected_date = parse(selected_month_year, dayfirst=False, yearfirst=True)
    start_of_month = datetime.datetime(selected_date.year, selected_date.month, 1, 0)
    end_of_month = start_of_month + relativedelta(months=1)

    merchant_oid = ObjectId(merchant_id)

    planes_mensuales = [plan['_id'] for plan in planes.find({
        'merchant_id' : merchant_oid,
        'cobro':'Mensual'
    })]
    planes_anuales = [plan['_id'] for plan in planes.find({
        'merchant_id' : merchant_oid,
        'cobro':'Anual'
    })]

    cobro_mensual = boletas.count_documents({
        'merchant_id' : merchant_oid,
        'date_created': {
            '$gte': start_of_month,
            '$lt': end_of_month
        },
        'plan_id': {'$in': planes_mensuales}    
    })
    cobro_anual = boletas.count_documents({
        'merchant_id' : merchant_oid,
        'date_created': {
            '$gte': start_of_month,
            '$lt': end_of_month
        },
        'plan_id': {'$in': planes_anuales}    
    })

    planes_locales = [plan['_id'] for plan in planes.find({
        'merchant_id' : merchant_oid,
        'nivel_de_acceso':'Local'
    })]
    planes_plus = [plan['_id'] for plan in planes.find({
        'merchant_id' : merchant_oid,
        'nivel_de_acceso':'Plus'
    })]
    planes_totales = [plan['_id'] for plan in planes.find({
        'merchant_id' : merchant_oid,
        'nivel_de_acceso':'Total'
    })]
    acceso_local = boletas.count_documents({
        'merchant_id' : merchant_oid,
        'date_created': {
            '$gte': start_of_month,
            '$lt': end_of_month
        },
        'plan_id': {'$in': planes_locales}    
    })
    acceso_plus = boletas.count_documents({
        'merchant_id' : merchant_oid,
        'date_created': {
            '$gte': start_of_month,
            '$lt': end_of_month
        },
        'plan_id': {'$in': planes_plus}    
    })
    acceso_total = boletas.count_documents({
        'merchant_id' : merchant_oid,
        'date_created': {
            '$gte': start_of_month,
            '$lt': end_of_month
        },
        'plan_id': {'$in': planes_totales}    
    })

    total_documentos = boletas.count_documents({

        'merchant_id' : merchant_oid,
        'date_created': {
            '$gte': start_of_month,
            '$lt': end_of_month
        }
    })
    if total_documentos != 0:
        porcentaje_cobro_mensual = cobro_mensual / total_documentos * 100
        porcentaje_cobro_anual = cobro_anual / total_documentos * 100
        porcentaje_acceso_local = acceso_local / total_documentos * 100
        porcentaje_acceso_plus = acceso_plus / total_documentos * 100
        porcentaje_acceso_total = acceso_total / total_documentos * 100
    else:
        porcentaje_cobro_mensual = 'n/c'
        porcentaje_cobro_anual = 'n/c'
        porcentaje_acceso_local = 'n/c'
        porcentaje_acceso_plus = 'n/c'
        porcentaje_acceso_total = 'n/c'


    res = {
        'porcentaje_cobro_mensual' : porcentaje_cobro_mensual,
        'porcentaje_cobro_anual' : porcentaje_cobro_anual,
        'porcentaje_acceso_local' : porcentaje_acceso_local,
        'porcentaje_acceso_plus' : porcentaje_acceso_plus,
        'porcentaje_acceso_total' : porcentaje_acceso_total,
    }
    return res