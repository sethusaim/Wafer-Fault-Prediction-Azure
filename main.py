import json
import os

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.templating import Jinja2Templates

from utils.create_containers import create_log_container
from utils.read_params import read_params
from wafer.model.load_production_model import load_prod_model
from wafer.model.prediction_from_model import prediction
from wafer.model.training_model import train_model
from wafer.validation_insertion.prediction_validation_insertion import pred_validation
from wafer.validation_insertion.train_validation_insertion import train_validation

os.putenv("LANG", "en_US.UTF-8")
os.putenv("LC_ALL", "en_US.UTF-8")

app = FastAPI()

config = read_params()

templates = Jinja2Templates(directory=config["templates"]["dir"])

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse(
        config["templates"]["index"], {"request": request}
    )


@app.get("/train")
async def trainRouteClient():
    try:
        raw_data_train_container_name = config["train_container"]["wafer_raw_data"]

        container = create_log_container()

        container.generate_log_containers(type="train")

        train_val = train_validation(container_name=raw_data_train_container_name)

        train_val.training_validation()

        training_model_obj = train_model()

        num_clusters = training_model_obj.training_model()

        load_prod_model_obj = load_prod_model(num_clusters=num_clusters)

        load_prod_model_obj.load_production_model()

    except Exception as e:
        return Response("Error Occurred! %s" % e)

    return Response("Training successfull!!")


@app.get("/predict")
async def predictRouteClient():
    try:
        raw_data_pred_container_name = config["train_container"]["wafer_raw_data"]

        container = create_log_container()

        container.generate_log_containers(type="pred")

        pred_val = pred_validation(raw_data_pred_container_name)

        pred_val.prediction_validation()

        pred = prediction()

        container, filename, json_predictions = pred.predict_from_model()

        return Response(
            f"prediction file created in {container} container with filename as {filename}, and few of the predictions are {str(json.loads(json_predictions))}"
        )

    except Exception as e:
        return Response("Error Occurred! %s" % e)


if __name__ == "__main__":
    host = config["app"]["host"]

    port = config["app"]["port"]

    uvicorn.run(app, host=host, port=port)
