from flask import Flask, request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import config
import model
import orm
import repository
import services


orm.start_mappers()
get_session = sessionmaker(bind=create_engine(config.get_postgres_uri()))
app = Flask(__name__)


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    line = model.OrderLine(
        request.json["orderid"],
        request.json["sku"],
        request.json["qty"],
    )

    try:
        batchref = services.allocate(line, repo, session)
    except (model.OutOfStock, services.InvalidSku) as e:
        return {"message": str(e)}, 400

    return {"batchref": batchref}, 201


@app.route("/deallocate", methods=["POST"])
def deallocate_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    orderid = request.json["orderid"]
    sku = request.json["sku"]
    batchref = services.deallocate(orderid, sku, repo, session)
    return {"batchref": batchref}, 201


@app.route("/batches", methods=["POST"])
def add_batch_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    ref = (request.json["ref"],)
    sku = (request.json["sku"],)
    qty = (request.json["qty"],)
    eta = (request.json["eta"],)
    batch_ref = services.add_batch(ref, sku, qty, eta, repo, session)
    return {"batchref": batch_ref}, 201


@app.route("/allocations", methods=["GET"])
def allocations_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    data = services.get_batches_with_allocations(repo, session)
    return data, 200
