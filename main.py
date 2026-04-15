from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

PROJECT_ID = "property-mgmt-backend"
DATASET = "property_mgmt"


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

app = FastAPI()

# CORS middleware tells the browser which cross-origin requests are allowed.
# Allowing all origins ("*") is fine for a classroom demo but should be
# restricted to specific domains in a real production application.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # accept requests from any origin
    allow_methods=["GET", "POST"],
    allow_headers=["*"],       # accept any request headers
)


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):

     try:
        results = list(bq.query(f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """, job_config=job_config).result())

     except Exception as e:
        raise HTTPException(
            status_code=404, detail="Property not found"
            )

     return dict(results[0])

@app.post("/properties")
def create_property(property: dict, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.properties`
        (address, city, state, monthly_rent, tenant_name)
        VALUES (@address, @city, @state, @monthly_rent, @tenant_name)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("address", "STRING", property.get("address")),
            bigquery.ScalarQueryParameter("city", "STRING", property.get("city")),
            bigquery.ScalarQueryParameter("state", "STRING", property.get("state")),
            bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property.get("monthly_rent")),
            bigquery.ScalarQueryParameter("tenant_name", "STRING", property.get("tenant_name")),
        ]
    )

    try:
        bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"message": "Property created successfully"}

@app.put("/properties/{property_id}")
def update_property(property_id: int, property: dict, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.properties`
        SET address = @address,
            city = @city,
            state = @state,
            monthly_rent = @monthly_rent,
            tenant_name = @tenant_name
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("address", "STRING", property.get("address")),
            bigquery.ScalarQueryParameter("city", "STRING", property.get("city")),
            bigquery.ScalarQueryParameter("state", "STRING", property.get("state")),
            bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property.get("monthly_rent")),
            bigquery.ScalarQueryParameter("tenant_name", "STRING", property.get("tenant_name")),
        ]
    )

    bq.query(query, job_config=job_config).result()

    return {"message": "Property updated successfully"}

@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    bq.query(query, job_config=job_config).result()

    return {"message": "Property deleted successfully"}

@app.get("/properties/{property_id}/income")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    results = bq.query(query, job_config=job_config).result()

    return [dict(row) for row in results]

@app.post("/properties/{property_id}/income")
def create_income(property_id: int, income: dict, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income`
        (property_id, amount, date, income_description)
        VALUES (@property_id, @amount, @date, @description)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", income.get("amount")),
            bigquery.ScalarQueryParameter("date", "DATE", income.get("date")),
            bigquery.ScalarQueryParameter("description", "STRING", income.get("description")),
        ]
    )

    bq.query(query, job_config=job_config).result()

    return {"message": "Income record created successfully"}

@app.get("/properties/{property_id}/expenses")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.expense`
        WHERE property_id = @property_id
        ORDER BY date DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    results = bq.query(query, job_config=job_config).result()

    return [dict(row) for row in results]

@app.post("/properties/{property_id}/expenses")
def create_expense(property_id: int, expense: dict, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expense`
        (property_id, amount, date, expense_description)
        VALUES (@property_id, @amount, @date, @description)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", expense.get("amount")),
            bigquery.ScalarQueryParameter("date", "DATE", expense.get("date")),
            bigquery.ScalarQueryParameter("description", "STRING", expense.get("description")),
        ]
    )

    bq.query(query, job_config=job_config).result()

    return {"message": "Expense record created successfully"}